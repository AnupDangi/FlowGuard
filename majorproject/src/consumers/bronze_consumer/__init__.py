"""Snowflake Bronze Layer Consumer

Kafka consumer that reads events from raw topics and batch-writes to Snowflake.
Handles schema evolution gracefully with VARIANT columns.
"""

__version__ = "1.0.0"
