"""
Event-Time Handling Utilities
Watermark strategies and time extraction for fraud detection pipeline.
"""
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.time import Duration
from typing import Dict, Any


class EventTimeExtractor(TimestampAssigner):
    """
    Extracts event_time field from transaction events.
    Used to assign timestamps for event-time processing.
    """

    def extract_timestamp(self, value: Dict[str, Any], record_timestamp: int) -> int:
        """
        Extract timestamp from event.
        
        Args:
            value: Event dictionary containing 'event_time' field (milliseconds)
            record_timestamp: Kafka record timestamp (fallback if event_time missing)
            
        Returns:
            Timestamp in milliseconds
        """
        return value.get("event_time", record_timestamp)


def create_watermark_strategy(max_out_of_orderness_seconds: int = 5,
                              idle_timeout_seconds: int = 60) -> WatermarkStrategy:
    """
    Create watermark strategy for handling late events.
    
    Args:
        max_out_of_orderness_seconds: Maximum expected delay in event arrival
        idle_timeout_seconds: Time to wait before marking stream as idle
        
    Returns:
        Configured WatermarkStrategy
        
    Watermark semantics:
        - Events can arrive up to max_out_of_orderness late
        - After idle_timeout, stream is marked idle (prevents blocking)
        - Watermarks enable time-based window operations
    """
    return (WatermarkStrategy
            .for_bounded_out_of_orderness(Duration.of_seconds(max_out_of_orderness_seconds))
            .with_timestamp_assigner(EventTimeExtractor())
            .with_idleness(Duration.of_seconds(idle_timeout_seconds)))


# Pre-configured strategies for common use cases

def strict_watermark_strategy() -> WatermarkStrategy:
    """
    Strict watermark: Expects events in near-perfect order.
    Use when: Events arrive in-order from a single partition.
    """
    return create_watermark_strategy(
        max_out_of_orderness_seconds=1,
        idle_timeout_seconds=30
    )


def relaxed_watermark_strategy() -> WatermarkStrategy:
    """
    Relaxed watermark: Tolerates significant out-of-order events.
    Use when: Events come from multiple sources or partitions.
    """
    return create_watermark_strategy(
        max_out_of_orderness_seconds=10,
        idle_timeout_seconds=120
    )


def no_watermark_strategy() -> WatermarkStrategy:
    """
    No watermarks: Disables event-time semantics.
    Use when: Processing time is sufficient or testing.
    Note: Time-based windows will not work correctly.
    """
    return WatermarkStrategy.no_watermarks()


# Configuration mapping for easy selection
WATERMARK_STRATEGIES = {
    "strict": strict_watermark_strategy,
    "default": create_watermark_strategy,
    "relaxed": relaxed_watermark_strategy,
    "none": no_watermark_strategy
}


def get_watermark_strategy(strategy_name: str = "default") -> WatermarkStrategy:
    """
    Get watermark strategy by name.
    
    Args:
        strategy_name: One of "strict", "default", "relaxed", "none"
        
    Returns:
        Configured WatermarkStrategy
    """
    strategy_func = WATERMARK_STRATEGIES.get(strategy_name, create_watermark_strategy)
    return strategy_func()
