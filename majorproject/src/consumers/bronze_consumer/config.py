"""Configuration for Bronze Layer Consumer

Environment-based configuration for Kafka, Snowflake, and batching parameters.
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
    enable_auto_commit: bool = False  # Manual commit after Snowflake write
    
    # Batching configuration
    batch_size: int = int(os.getenv('BATCH_SIZE', '100'))  # 100 events
    batch_timeout_seconds: float = float(os.getenv('BATCH_TIMEOUT', '5.0'))  # 5 seconds
    
    # Snowflake settings
    snowflake_account: str = os.getenv('SNOWFLAKE_ACCOUNT', 'ZLNJTCF-KE38237')
    snowflake_user: str = os.getenv('SNOWFLAKE_USER', 'ANUPDANGI12')
    snowflake_password: str = os.getenv('SNOWFLAKE_PASSWORD', '')  # Required
    snowflake_warehouse: str = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
    snowflake_database: str = os.getenv('SNOWFLAKE_DATABASE', 'FLOWGUARD_DB')
    snowflake_schema: str = os.getenv('SNOWFLAKE_SCHEMA', 'BRONZE')
    
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
    
    def to_snowflake_config(self) -> dict:
        """Convert to snowflake-connector config"""
        if not self.snowflake_password:
            raise ValueError("SNOWFLAKE_PASSWORD environment variable required")
        
        return {
            'account': self.snowflake_account,
            'user': self.snowflake_user,
            'password': self.snowflake_password,
            'warehouse': self.snowflake_warehouse,
            'database': self.snowflake_database,
            'schema': self.snowflake_schema,
            'numpy': False,  # Disable numpy for simpler data types
        }
    
    def validate(self):
        """Validate configuration"""
        errors = []
        
        if not self.snowflake_password:
            errors.append("SNOWFLAKE_PASSWORD environment variable required")
        
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
