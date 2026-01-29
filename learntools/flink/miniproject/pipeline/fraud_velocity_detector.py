"""
Velocity-based Fraud Detector
Tracks order frequency per user within a time window.
Returns velocity risk signal based on order count threshold.
"""
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ListStateDescriptor

logger = logging.getLogger(__name__)

# Configuration
VELOCITY_WINDOW_MS = 60_000          # 60 seconds
VELOCITY_ORDER_THRESHOLD = 3         # max orders per user
VELOCITY_RISK_WEIGHT = 60            # risk score if threshold exceeded


class VelocityFraudDetector(KeyedProcessFunction):
    """
    Keyed by user_id. Tracks order timestamps within window.
    Flags users with abnormally high order velocity as risky.
    """

    def open(self, ctx):
        # List of event timestamps for this user
        self.events = ctx.get_list_state(
            ListStateDescriptor(
                "event_times",
                Types.LONG()
            )
        )

    def process_element(self, event, ctx):
        event_time = event["event_time"]

        # Add current event time
        self.events.add(event_time)

        # Remove stale events outside the window
        valid_events = []
        for ts in self.events.get():
            if ts >= event_time - VELOCITY_WINDOW_MS:
                valid_events.append(ts)

        # Update state with only recent events
        self.events.update(valid_events)

        order_count = len(valid_events)

        # Calculate velocity risk score
        velocity_risk = VELOCITY_RISK_WEIGHT if order_count > VELOCITY_ORDER_THRESHOLD else 0
        
        if velocity_risk > 0:
            logger.warning(
                f"⚠️  VELOCITY ALERT: user={event.get('user_id')} | "
                f"orders={order_count} (threshold={VELOCITY_ORDER_THRESHOLD}) | "
                f"risk_score={velocity_risk}"
            )
        else:
            logger.debug(f"✓ Velocity OK: user={event.get('user_id')}, orders={order_count}")

        # Return enriched event with velocity signals
        yield {
            **event,  # preserve original event fields
            "velocity_order_count": order_count,
            "velocity_risk_score": velocity_risk,
            "velocity_threshold_exceeded": order_count > VELOCITY_ORDER_THRESHOLD
        }


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("zomato_transactions_raw") \
        .set_group_id("fraud-detector") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka Source"
    )

    events = stream.map(lambda x: json.loads(x))

    decisions = (
        events
        .key_by(lambda e: e["user_id"])
        .process(VelocityFraudDetector())
    )

    decisions.print()

    env.execute("Velocity Fraud Detection")


if __name__ == "__main__":
    main()
