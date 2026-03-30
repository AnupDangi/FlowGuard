"""PostgreSQL analytics writer for Bronze layer.

This module keeps the legacy filename (`snowflake_writer.py`) to minimize import churn
while migrating from Snowflake to local PostgreSQL analytics.
"""

import json
import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import execute_batch

from .config import get_config

logger = logging.getLogger(__name__)


def _parse_timestamp(ts_value) -> Optional[datetime]:
    """Parse supported timestamp values to Python datetime."""
    if ts_value is None:
        return None
    if isinstance(ts_value, datetime):
        return ts_value
    if isinstance(ts_value, str):
        try:
            return datetime.fromisoformat(ts_value.replace("Z", "+00:00"))
        except ValueError:
            logger.warning("Could not parse timestamp: %r", ts_value)
            return None
    return None


class SnowflakeWriterError(Exception):
    """Compatibility error type for Bronze consumer."""


class SnowflakeWriter:
    """Bulk writer for PostgreSQL BRONZE tables with idempotent upserts."""

    def __init__(self):
        self.config = get_config()
        self._connection = None
        self._connect()

    def _connect(self):
        try:
            if self._connection:
                try:
                    self._connection.close()
                except Exception:
                    pass
            self._connection = psycopg2.connect(**self.config.to_analytics_postgres_config())
            self._connection.autocommit = False
            logger.info(
                "Connected to analytics Postgres: %s:%s/%s",
                self.config.analytics_host,
                self.config.analytics_port,
                self.config.analytics_db,
            )
        except Exception as e:
            logger.error("Failed to connect to analytics Postgres: %s", e)
            raise SnowflakeWriterError(f"Connection failed: {e}")

    @contextmanager
    def _get_cursor(self):
        cursor = None
        try:
            if self._connection is None or self._connection.closed:
                self._connect()
            cursor = self._connection.cursor()
            yield cursor
            self._connection.commit()
        except Exception as e:
            if self._connection:
                try:
                    self._connection.rollback()
                except Exception:
                    pass
            if "closed" in str(e).lower() or "connection" in str(e).lower():
                logger.warning("Connection issue detected, reconnecting on next attempt: %s", e)
                try:
                    self._connect()
                except Exception:
                    pass
            raise
        finally:
            if cursor:
                cursor.close()

    def write_orders_batch(self, events: List[tuple]) -> int:
        """Write orders into analytics.bronze_orders_raw."""
        if not events:
            return 0
        try:
            with self._get_cursor() as cursor:
                rows = []
                for event, metadata in events:
                    ts = _parse_timestamp(event.get("timestamp"))
                    rows.append(
                        (
                            event.get("event_id"),
                            event.get("order_id"),
                            event.get("user_id"),
                            event.get("item_id"),
                            event.get("item_name"),
                            event.get("price"),
                            event.get("status", "confirmed"),
                            ts,
                            json.dumps(event),
                            event.get("event_type", "order"),
                            metadata.get("schema_version"),
                            metadata.get("source_topic"),
                            metadata.get("producer_service"),
                            metadata.get("ingestion_id"),
                            metadata.get("kafka_partition"),
                            metadata.get("kafka_offset"),
                        )
                    )

                sql = """
                    INSERT INTO analytics.bronze_orders_raw (
                        event_id, order_id, user_id, item_id, item_name, price, status,
                        event_timestamp, raw_event, event_type, schema_version,
                        source_topic, producer_service, ingestion_id, kafka_partition, kafka_offset
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s::jsonb, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (ingestion_id) DO NOTHING
                """
                execute_batch(cursor, sql, rows, page_size=500)
                inserted = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                if inserted < len(events):
                    logger.info("Merged %s orders (skipped %s duplicates)", inserted, len(events) - inserted)
                else:
                    logger.info("Inserted %s orders into analytics Postgres", inserted)
                return inserted
        except Exception as e:
            logger.error("Error writing orders batch: %s", e)
            raise SnowflakeWriterError(f"Write failed: {e}")

    def write_clicks_batch(self, events: List[tuple]) -> int:
        """Write clicks/impressions into analytics.bronze_clicks_raw."""
        if not events:
            return 0
        try:
            with self._get_cursor() as cursor:
                rows = []
                for event, metadata in events:
                    ts = _parse_timestamp(event.get("timestamp"))
                    rows.append(
                        (
                            event.get("event_id"),
                            event.get("user_id"),
                            event.get("event_type"),
                            event.get("item_id"),
                            event.get("session_id"),
                            ts,
                            json.dumps(event),
                            metadata.get("schema_version"),
                            metadata.get("source_topic"),
                            metadata.get("producer_service"),
                            metadata.get("ingestion_id"),
                            metadata.get("kafka_partition"),
                            metadata.get("kafka_offset"),
                        )
                    )
                sql = """
                    INSERT INTO analytics.bronze_clicks_raw (
                        event_id, user_id, event_type, item_id, session_id,
                        event_timestamp, raw_event, schema_version,
                        source_topic, producer_service, ingestion_id, kafka_partition, kafka_offset
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s::jsonb, %s,
                        %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (ingestion_id) DO NOTHING
                """
                execute_batch(cursor, sql, rows, page_size=500)
                inserted = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                if inserted < len(events):
                    logger.info("Merged %s clicks (skipped %s duplicates)", inserted, len(events) - inserted)
                else:
                    logger.info("Inserted %s clicks into analytics Postgres", inserted)
                return inserted
        except Exception as e:
            logger.error("Error writing clicks batch: %s", e)
            raise SnowflakeWriterError(f"Write failed: {e}")

    def test_connection(self) -> bool:
        try:
            with self._get_cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False

    def close(self):
        if self._connection:
            try:
                self._connection.close()
                logger.info("Analytics Postgres connection closed")
            except Exception as e:
                logger.error("Error closing connection: %s", e)


_writer_instance: SnowflakeWriter = None


def get_writer() -> SnowflakeWriter:
    global _writer_instance
    if _writer_instance is None:
        _writer_instance = SnowflakeWriter()
    return _writer_instance


def close_writer():
    global _writer_instance
    if _writer_instance is not None:
        _writer_instance.close()
        _writer_instance = None
