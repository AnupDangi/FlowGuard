"""Configuration for Bronze Layer Consumer.

Environment-based configuration for Kafka, PostgreSQL analytics, and batching parameters.
"""

import os
from pathlib import Path
from dataclasses import dataclass
from typing import List
from dotenv import load_dotenv

# Load .env file from project root
project_root = Path(__file__).parent.parent.parent.parent
load_dotenv(project_root / '.env')


@dataclass
class ConsumerConfig:
    """Kafka consumer configuration"""
    
    # Kafka settings
    bootstrap_servers: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:19092,localhost:19093,localhost:19094')
    consumer_group: str = os.getenv('CONSUMER_GROUP_ID', 'bronze-consumer-group')
    topics: List[str] = None  # Set in __post_init__
    auto_offset_reset: str = 'earliest'  # Start from beginning for new consumer group
    enable_auto_commit: bool = False  # Manual commit after analytics DB write
    
    # Batching configuration
    batch_size: int = int(os.getenv('BATCH_SIZE', '100'))  # 100 events
    batch_timeout_seconds: float = float(os.getenv('BATCH_TIMEOUT', '300.0'))  # 5 minutes (300 seconds)
    batch_max_size_mb: float = float(os.getenv('BATCH_MAX_SIZE_MB', '128.0'))  # 128 MB max batch size
    
    # PostgreSQL analytics settings
    analytics_host: str = os.getenv('ANALYTICS_POSTGRES_HOST', 'localhost')
    analytics_port: int = int(os.getenv('ANALYTICS_POSTGRES_PORT', '5434'))
    analytics_user: str = os.getenv('ANALYTICS_POSTGRES_USER', 'flowguard')
    analytics_password: str = os.getenv('ANALYTICS_POSTGRES_PASSWORD', '')
    analytics_db: str = os.getenv('ANALYTICS_POSTGRES_DB', 'flowguard_analytics')
    
    # Retry configuration
    max_retries: int = 3
    retry_delay_seconds: float = 2.0
    
    # Logging
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')
    
    def __post_init__(self):
        """Initialize topics list"""
        if self.topics is None:
            self.topics = [
                'raw.orders.v1',
                'raw.clicks.v1'
            ]
    
    def to_kafka_config(self) -> dict:
        """Convert to kafka-python consumer config"""
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.consumer_group,
            'auto.offset.reset': self.auto_offset_reset,
            'enable.auto.commit': self.enable_auto_commit,
            'max.poll.interval.ms': 300000,  # 5 minutes
            'session.timeout.ms': 30000,  # 30 seconds
        }
    
    def to_analytics_postgres_config(self) -> dict:
        """Convert to psycopg2 connection kwargs."""
        if not self.analytics_password:
            raise ValueError("ANALYTICS_POSTGRES_PASSWORD environment variable required")

        return {
            'host': self.analytics_host,
            'port': self.analytics_port,
            'user': self.analytics_user,
            'password': self.analytics_password,
            'dbname': self.analytics_db,
        }
    
    def validate(self):
        """Validate configuration"""
        errors = []
        
        if not self.analytics_password:
            errors.append("ANALYTICS_POSTGRES_PASSWORD environment variable required")
        
        if self.batch_size <= 0:
            errors.append(f"Invalid batch_size: {self.batch_size}")
        
        if self.batch_timeout_seconds <= 0:
            errors.append(f"Invalid batch_timeout: {self.batch_timeout_seconds}")
        
        if errors:
            raise ValueError(f"Configuration errors: {', '.join(errors)}")


# Global config instance
_config: ConsumerConfig = None


def get_config() -> ConsumerConfig:
    """Get or create global config instance"""
    global _config
    if _config is None:
        _config = ConsumerConfig()
        _config.validate()
    return _config
