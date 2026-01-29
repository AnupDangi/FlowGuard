"""
Device-based Fraud Detector
Tracks unique users per device within a time window.
Returns device risk signal based on user count threshold.
"""
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import MapStateDescriptor

logger = logging.getLogger(__name__)

# Configuration
DEVICE_WINDOW_MS = 5 * 60_000        # 5 minutes
DEVICE_USER_THRESHOLD = 3            # max unique users per device
DEVICE_RISK_WEIGHT = 40              # risk score if threshold exceeded


class DeviceFraudDetector(KeyedProcessFunction):
    """
    Keyed by device_id. Tracks unique users on each device.
    Flags devices with suspiciously high user count as risky.
    """

    def open(self, ctx):
        # Map: user_id -> last_seen_timestamp
        self.user_state = ctx.get_map_state(
            MapStateDescriptor(
                "device_users",
                Types.STRING(),
                Types.LONG()
            )
        )

    def process_element(self, event, ctx):
        device_id = event["device_id"]
        user_id = event["user_id"]
        event_time = event["event_time"]

        # Record this user's activity on this device
        self.user_state.put(user_id, event_time)

        # Cleanup stale users outside the window
        active_users = []
        for u, ts in self.user_state.items():
            if ts >= event_time - DEVICE_WINDOW_MS:
                active_users.append(u)
            else:
                self.user_state.remove(u)

        user_count = len(active_users)

        # Calculate device risk score
        device_risk = DEVICE_RISK_WEIGHT if user_count > DEVICE_USER_THRESHOLD else 0
        
        if device_risk > 0:
            logger.warning(
                f"⚠️  DEVICE ALERT: device={device_id} | "
                f"unique_users={user_count} (threshold={DEVICE_USER_THRESHOLD}) | "
                f"risk_score={device_risk}"
            )
        else:
            logger.debug(f"✓ Device OK: device={device_id}, users={user_count}")

        # Return enriched event with device signals
        yield {
            **event,  # preserve original event fields
            "device_unique_users": user_count,
            "device_risk_score": device_risk,
            "device_threshold_exceeded": user_count > DEVICE_USER_THRESHOLD
        }


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("zomato_transactions_raw") \
        .set_group_id("device-fraud-detector") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka Source"
    )

    events = stream.map(lambda x: json.loads(x))

    device_results = (
        events
        .key_by(lambda e: e["device_id"])
        .process(DeviceFraudDetector())
    )

    device_results.print()

    env.execute("Device Based Fraud Detection")


if __name__ == "__main__":
    main()
