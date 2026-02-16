"""Snowflake Writer for Bronze Layer

Handles bulk inserts to Snowflake with connection pooling and error handling.
"""

import json
import logging
from typing import List, Dict, Any
from datetime import datetime
from contextlib import contextmanager

import snowflake.connector
from snowflake.connector.errors import ProgrammingError, DatabaseError

from .config import get_config

logger = logging.getLogger(__name__)


class SnowflakeWriterError(Exception):
    """Custom exception for Snowflake writer errors"""
    pass


class SnowflakeWriter:
    """Bulk writer for Snowflake Bronze tables"""
    
    def __init__(self):
        """Initialize Snowflake connection"""
        self.config = get_config()
        self._connection = None
        self._last_connection_check = 0
        self._connection_check_interval = 60  # Check connection every 60 seconds
        self._connect()
    
    def _connect(self):
        """Establish Snowflake connection"""
        try:
            if self._connection:
                try:
                    self._connection.close()
                except:
                    pass
            
            self._connection = snowflake.connector.connect(
                **self.config.to_snowflake_config()
            )
            logger.info(
                f"Connected to Snowflake: {self.config.snowflake_database}.{self.config.snowflake_schema}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise SnowflakeWriterError(f"Connection failed: {e}")
    
    def _ensure_connection(self):
        """Ensure connection is alive, reconnect if needed"""
        import time
        current_time = time.time()
        
        # Only check periodically to avoid overhead
        if current_time - self._last_connection_check > self._connection_check_interval:
            try:
                # Quick connection test
                cursor = self._connection.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                self._last_connection_check = current_time
            except Exception as e:
                logger.warning(f"Connection check failed, reconnecting: {e}")
                self._connect()
                self._last_connection_check = current_time
    
    @contextmanager
    def _get_cursor(self):
        """Get cursor with auto-commit"""
        cursor = None
        try:
            # Ensure connection is alive before getting cursor
            self._ensure_connection()
            
            cursor = self._connection.cursor()
            yield cursor
            self._connection.commit()
        except Exception as e:
            if self._connection:
                try:
                    self._connection.rollback()
                except:
                    pass
            # If it's a connection error, try to reconnect
            if 'connection' in str(e).lower() or 'session' in str(e).lower():
                logger.warning(f"Connection error detected, will reconnect on next operation: {e}")
                try:
                    self._connect()
                except:
                    pass
            raise e
        finally:
            if cursor:
                cursor.close()
    
    def write_orders_batch(self, events: List[tuple]) -> int:
        """Write batch of order events to ORDERS_RAW table with idempotency
        
        Args:
            events: List of (event_dict, metadata_dict) tuples
            
        Returns:
            int: Number of rows inserted/updated
            
        Raises:
            SnowflakeWriterError: If write fails
        """
        if not events:
            return 0
        
        try:
            with self._get_cursor() as cursor:
                # Step 1: Create temporary table
                cursor.execute("""
                    CREATE TEMPORARY TABLE ORDERS_STAGING (
                        EVENT_ID VARCHAR,
                        ORDER_ID VARCHAR,
                        USER_ID VARCHAR,
                        ITEM_ID VARCHAR,
                        ITEM_NAME VARCHAR,
                        PRICE FLOAT,
                        STATUS VARCHAR,
                        EVENT_TIMESTAMP TIMESTAMP_NTZ,
                        RAW_EVENT VARIANT,
                        EVENT_TYPE VARCHAR,
                        SCHEMA_VERSION VARCHAR,
                        SOURCE_TOPIC VARCHAR,
                        PRODUCER_SERVICE VARCHAR,
                        INGESTION_ID VARCHAR PRIMARY KEY
                    )
                """)
                
                # Step 2: Bulk insert into staging table
                insert_staging = """
                    INSERT INTO ORDERS_STAGING 
                    SELECT %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, %s, %s, %s, %s
                """
                
                rows = []
                for event, metadata in events:
                    row = (
                        event.get('event_id'),
                        event.get('order_id'),
                        event.get('user_id'),
                        event.get('item_id'),
                        event.get('item_name'),
                        event.get('price'),
                        event.get('status', 'confirmed'),
                        event.get('timestamp'),
                        json.dumps(event),
                        event.get('event_type', 'order'),
                        metadata.get('schema_version'),
                        metadata.get('source_topic'),
                        metadata.get('producer_service'),
                        metadata.get('ingestion_id')
                    )
                    rows.append(row)
                
                # Bulk insert all rows
                cursor.executemany(insert_staging, rows)
                
                # Step 3: Single MERGE from staging to target
                merge_query = """
                    MERGE INTO ORDERS_RAW AS target
                    USING ORDERS_STAGING AS source
                    ON target.INGESTION_ID = source.INGESTION_ID
                    WHEN NOT MATCHED THEN
                        INSERT (
                            EVENT_ID, ORDER_ID, USER_ID, ITEM_ID, ITEM_NAME, PRICE, STATUS,
                            EVENT_TIMESTAMP, RAW_EVENT, EVENT_TYPE, SCHEMA_VERSION, 
                            SOURCE_TOPIC, PRODUCER_SERVICE, INGESTION_ID
                        )
                        VALUES (
                            source.EVENT_ID, source.ORDER_ID, source.USER_ID, source.ITEM_ID,
                            source.ITEM_NAME, source.PRICE, source.STATUS, source.EVENT_TIMESTAMP,
                            source.RAW_EVENT, source.EVENT_TYPE, source.SCHEMA_VERSION,
                            source.SOURCE_TOPIC, source.PRODUCER_SERVICE, source.INGESTION_ID
                        )
                """
                cursor.execute(merge_query)
                inserted = cursor.rowcount
                skipped = len(events) - inserted
                
                # Step 4: Drop staging table
                cursor.execute("DROP TABLE ORDERS_STAGING")
                
                if skipped > 0:
                    logger.info(f"Merged {inserted} orders (skipped {skipped} duplicates)")
                else:
                    logger.info(f"Inserted {inserted} orders into Snowflake")
                return inserted
                
        except (ProgrammingError, DatabaseError) as e:
            logger.error(f"Snowflake write error: {e}")
            raise SnowflakeWriterError(f"Write failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error writing to Snowflake: {e}")
            raise SnowflakeWriterError(f"Unexpected error: {e}")
    
    def write_clicks_batch(self, events: List[tuple]) -> int:
        """Write batch of click events to CLICKS_RAW table with idempotency
        
        Args:
            events: List of (event_dict, metadata_dict) tuples
            
        Returns:
            int: Number of rows inserted
            
        Raises:
            SnowflakeWriterError: If write fails
        """
        if not events:
            return 0
        
        try:
            with self._get_cursor() as cursor:
                # Step 1: Create temporary table
                cursor.execute("""
                    CREATE TEMPORARY TABLE CLICKS_STAGING (
                        EVENT_ID VARCHAR,
                        USER_ID VARCHAR,
                        EVENT_TYPE VARCHAR,
                        ITEM_ID VARCHAR,
                        SESSION_ID VARCHAR,
                        EVENT_TIMESTAMP TIMESTAMP_NTZ,
                        RAW_EVENT VARIANT,
                        SCHEMA_VERSION VARCHAR,
                        SOURCE_TOPIC VARCHAR,
                        PRODUCER_SERVICE VARCHAR,
                        INGESTION_ID VARCHAR PRIMARY KEY
                    )
                """)
                
                # Step 2: Bulk insert into staging table
                insert_staging = """
                    INSERT INTO CLICKS_STAGING 
                    SELECT %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, %s, %s, %s
                """
                
                rows = []
                for event, metadata in events:
                    row = (
                        event.get('event_id'),
                        event.get('user_id'),
                        event.get('event_type'),
                        event.get('item_id'),
                        event.get('session_id'),
                        event.get('timestamp'),
                        json.dumps(event),
                        metadata.get('schema_version'),
                        metadata.get('source_topic'),
                        metadata.get('producer_service'),
                        metadata.get('ingestion_id')
                    )
                    rows.append(row)
                
                # Bulk insert all rows
                cursor.executemany(insert_staging, rows)
                
                # Step 3: Single MERGE from staging to target (now with idempotency!)
                merge_query = """
                    MERGE INTO CLICKS_RAW AS target
                    USING CLICKS_STAGING AS source
                    ON target.INGESTION_ID = source.INGESTION_ID
                    WHEN NOT MATCHED THEN
                        INSERT (
                            EVENT_ID, USER_ID, EVENT_TYPE, ITEM_ID, SESSION_ID,
                            EVENT_TIMESTAMP, RAW_EVENT, SCHEMA_VERSION,
                            SOURCE_TOPIC, PRODUCER_SERVICE, INGESTION_ID
                        )
                        VALUES (
                            source.EVENT_ID, source.USER_ID, source.EVENT_TYPE,
                            source.ITEM_ID, source.SESSION_ID, source.EVENT_TIMESTAMP,
                            source.RAW_EVENT, source.SCHEMA_VERSION, source.SOURCE_TOPIC,
                            source.PRODUCER_SERVICE, source.INGESTION_ID
                        )
                """
                cursor.execute(merge_query)
                inserted = cursor.rowcount
                skipped = len(events) - inserted
                
                # Step 4: Drop staging table
                cursor.execute("DROP TABLE CLICKS_STAGING")
                
                if skipped > 0:
                    logger.info(f"Merged {inserted} clicks (skipped {skipped} duplicates)")
                else:
                    logger.info(f"Inserted {inserted} clicks into Snowflake")
                return inserted
                
        except (ProgrammingError, DatabaseError) as e:
            logger.error(f"Snowflake write error: {e}")
            raise SnowflakeWriterError(f"Write failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error writing to Snowflake: {e}")
            raise SnowflakeWriterError(f"Unexpected error: {e}")
    
    def test_connection(self) -> bool:
        """Test Snowflake connection
        
        Returns:
            bool: True if connection successful
        """
        try:
            with self._get_cursor() as cursor:
                cursor.execute("SELECT CURRENT_VERSION()")
                version = cursor.fetchone()[0]
                logger.info(f"Snowflake connection OK. Version: {version}")
                return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def close(self):
        """Close Snowflake connection"""
        if self._connection:
            try:
                self._connection.close()
                logger.info("Snowflake connection closed")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")


# Global writer instance
_writer_instance: SnowflakeWriter = None


def get_writer() -> SnowflakeWriter:
    """Get or create global writer instance"""
    global _writer_instance
    if _writer_instance is None:
        _writer_instance = SnowflakeWriter()
    return _writer_instance


def close_writer():
    """Close global writer instance"""
    global _writer_instance
    if _writer_instance is not None:
        _writer_instance.close()
        _writer_instance = None
