"""Kafka Consumer for Bronze Layer

Consumes events from raw topics and batch-writes to Snowflake.
Implements micro-batching with time and size limits.
"""

import json
import logging
import time
from typing import List, Dict, Any
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from .config import get_config
from .snowflake_writer import get_writer, SnowflakeWriterError

logger = logging.getLogger(__name__)


class BronzeConsumer:
    """Kafka consumer for Bronze layer with batching"""
    
    def __init__(self):
        """Initialize consumer and writer"""
        self.config = get_config()
        self.writer = get_writer()
        self._consumer = None
        self._running = False
        
        # Batching state
        self._orders_batch: List[Dict[str, Any]] = []
        self._clicks_batch: List[Dict[str, Any]] = []
        self._batch_start_time = time.time()
        
        # Statistics
        self._messages_consumed = 0
        self._messages_written = 0
        self._batches_written = 0
        self._errors = 0
        
        self._init_consumer()
    
    def _init_consumer(self):
        """Initialize Kafka consumer"""
        try:
            self._consumer = Consumer(self.config.to_kafka_config())
            self._consumer.subscribe(self.config.topics)
            logger.info(
                f"Consumer initialized: group={self.config.consumer_group}, "
                f"topics={self.config.topics}"
            )
        except KafkaException as e:
            logger.error(f"Failed to initialize consumer: {e}")
            raise
    
    def _should_flush_batch(self) -> bool:
        """Check if batch should be flushed based on size or time"""
        total_events = len(self._orders_batch) + len(self._clicks_batch)
        
        # Size limit reached
        if total_events >= self.config.batch_size:
            logger.debug(f"Batch size limit reached: {total_events} events")
            return True
        
        # Time limit reached
        elapsed = time.time() - self._batch_start_time
        if elapsed >= self.config.batch_timeout_seconds:
            logger.debug(f"Batch timeout reached: {elapsed:.2f}s")
            return True
        
        return False
    
    def _flush_batches(self) -> bool:
        """Write batches to Snowflake and commit offsets
        
        Returns:
            bool: True if successful, False if error
        """
        if not self._orders_batch and not self._clicks_batch:
            return True
        
        try:
            # Write orders batch
            if self._orders_batch:
                logger.info(f"Flushing {len(self._orders_batch)} orders to Snowflake...")
                inserted = self.writer.write_orders_batch(self._orders_batch)
                self._messages_written += inserted
                self._batches_written += 1
            
            # Write clicks batch
            if self._clicks_batch:
                logger.info(f"Flushing {len(self._clicks_batch)} clicks to Snowflake...")
                inserted = self.writer.write_clicks_batch(self._clicks_batch)
                self._messages_written += inserted
                self._batches_written += 1
            
            # ✅ Commit offsets only after successful Snowflake write
            self._consumer.commit(asynchronous=False)
            logger.info("Kafka offsets committed")
            
            # Reset batches
            self._orders_batch = []
            self._clicks_batch = []
            self._batch_start_time = time.time()
            
            return True
            
        except SnowflakeWriterError as e:
            logger.error(f"Failed to write batch to Snowflake: {e}")
            self._errors += 1
            
            # TODO: Implement DLQ (Dead Letter Queue) for failed batches
            # For now, we'll retry on next batch
            return False
        except Exception as e:
            logger.error(f"Unexpected error flushing batch: {e}")
            self._errors += 1
            return False
    
    def _process_message(self, msg):
        """Process single Kafka message
        
        Args:
            msg: Kafka message
        """
        try:
            # Parse JSON event
            event = json.loads(msg.value().decode('utf-8'))
            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()
            
            # Add production metadata
            kafka_metadata = {
                'source_topic': topic,
                'producer_service': 'events-gateway',  # from upstream service
                'schema_version': 'v1',
                'ingestion_id': f"{topic}-{partition}-{offset}",  # Idempotency key
                'kafka_partition': partition,
                'kafka_offset': offset
            }
            
            # Route to appropriate batch
            if topic == 'raw.orders.v1':
                self._orders_batch.append((event, kafka_metadata))
            elif topic == 'raw.clicks.v1':
                self._clicks_batch.append((event, kafka_metadata))
            else:
                logger.warning(f"Unknown topic: {topic}")
                return
            
            self._messages_consumed += 1
            
            # Check if batch should be flushed
            if self._should_flush_batch():
                self._flush_batches()
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in message: {e}")
            self._errors += 1
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self._errors += 1
    
    def start(self):
        """Start consuming messages"""
        self._running = True
        logger.info("Starting Bronze consumer...")
        
        try:
            while self._running:
                # Poll for messages (1 second timeout)
                msg = self._consumer.poll(timeout=1.0)
                
                if msg is None:
                    # No message, check if batch timeout reached
                    if self._should_flush_batch():
                        self._flush_batches()
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, not an error
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                # Process message
                self._process_message(msg)
                
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """Stop consumer and flush remaining batches"""
        if not self._running:
            return
        
        logger.info("Stopping Bronze consumer...")
        self._running = False
        
        try:
            # Flush remaining batches
            logger.info("Flushing remaining batches...")
            self._flush_batches()
            
            # Close consumer
            if self._consumer:
                self._consumer.close()
                logger.info("Consumer closed")
            
            # Log statistics
            logger.info(
                f"Consumer statistics: "
                f"Consumed={self._messages_consumed}, "
                f"Written={self._messages_written}, "
                f"Batches={self._batches_written}, "
                f"Errors={self._errors}"
            )
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    def get_stats(self) -> Dict[str, int]:
        """Get consumer statistics"""
        return {
            'messages_consumed': self._messages_consumed,
            'messages_written': self._messages_written,
            'batches_written': self._batches_written,
            'errors': self._errors,
            'orders_batch_size': len(self._orders_batch),
            'clicks_batch_size': len(self._clicks_batch)
        }
