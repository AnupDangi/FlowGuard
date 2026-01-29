"""
Real-Time Fraud Detection Pipeline
Combines velocity, device, and ML signals for transaction fraud detection.

Pipeline flow:
    Kafka → Parse → Velocity Check → Device Check → Risk Combine → Decision → Output
"""
import json
import logging
import sys
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Import fraud detection components
from fraud_velocity_detector import VelocityFraudDetector
from fraud_device_detector import DeviceFraudDetector
from risk_combiner import RiskCombiner, MLScoringHook
from decision_maker import DecisionMaker, DecisionRouter
from event_time_utils import create_watermark_strategy

# ---- CONFIGURATION ----
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "zomato_transactions_raw"
OUTPUT_TOPIC = "fraud_decisions"
CONSUMER_GROUP = "fraud-decision-pipeline"

# Risk scoring weights
VELOCITY_WEIGHT = 1.0
DEVICE_WEIGHT = 1.0
ML_WEIGHT = 0.0  # Disabled by default

# Decision thresholds
LOW_RISK_THRESHOLD = 40   # Below this: ALLOW
HIGH_RISK_THRESHOLD = 70  # Above this: BLOCK
# Between thresholds: FLAG for manual review

# Event-time handling
MAX_OUT_OF_ORDERNESS_SECONDS = 5
IDLE_TIMEOUT_SECONDS = 60
# ------------------------


def parse_json(json_str: str):
    """Parse JSON string to dictionary"""
    try:
        event = json.loads(json_str)
        logger.debug(f"✓ Parsed event: order_id={event.get('order_id', 'N/A')}")
        return event
    except Exception as e:
        logger.error(f"✗ JSON parse error: {e}, raw={json_str[:100]}")
        return None


def to_json(event: dict) -> str:
    """Convert dictionary to JSON string"""
    return json.dumps(event)


def build_pipeline(env: StreamExecutionEnvironment):
    """
    Build the complete fraud detection pipeline.
    
    Pipeline stages:
        1. Ingest from Kafka
        2. Assign event-time watermarks
        3. Velocity detection (per user)
        4. Device detection (per device)  
        5. Risk combination (aggregate scores)
        6. ML scoring hook (future)
        7. Decision making (ALLOW/FLAG/BLOCK)
        8. Decision routing (metadata)
        9. Output to Kafka
    """
    
    # ---- STAGE 1: Kafka Source ----
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_topics(INPUT_TOPIC) \
        .set_group_id(CONSUMER_GROUP) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    # ---- STAGE 2: Ingest & Parse ----
    watermark_strategy = create_watermark_strategy(
        max_out_of_orderness_seconds=MAX_OUT_OF_ORDERNESS_SECONDS,
        idle_timeout_seconds=IDLE_TIMEOUT_SECONDS
    )
    
    raw_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=watermark_strategy,
        source_name="Kafka Transaction Source"
    )
    
    # Parse JSON events
    events = raw_stream \
        .map(parse_json) \
        .filter(lambda x: x is not None)  # Filter out invalid JSON
    
    # ---- STAGE 3: Velocity Detection ----
    # Keyed by user_id to track per-user order velocity
    velocity_enriched = (
        events
        .key_by(lambda e: e["user_id"])
        .process(VelocityFraudDetector())
    )
    
    # ---- STAGE 4: Device Detection ----
    # Re-key by device_id to track per-device user activity
    device_enriched = (
        velocity_enriched
        .key_by(lambda e: e["device_id"])
        .process(DeviceFraudDetector())
    )
    
    # ---- STAGE 5: Risk Combination ----
    # Combine velocity + device + ML scores
    ml_hook = MLScoringHook(model_endpoint=None)  # Disabled for now
    
    risk_scored = device_enriched.map(
        lambda event: {
            **event,
            "ml_risk_score": ml_hook.predict(event)
        }
    ).map(
        RiskCombiner(
            velocity_weight=VELOCITY_WEIGHT,
            device_weight=DEVICE_WEIGHT,
            ml_weight=ML_WEIGHT
        )
    )
    
    # ---- STAGE 6: Decision Making ----
    decisions = risk_scored.map(
        DecisionMaker(
            low_threshold=LOW_RISK_THRESHOLD,
            high_threshold=HIGH_RISK_THRESHOLD
        )
    )
    
    # ---- STAGE 7: Decision Routing ----
    routed_decisions = decisions.map(DecisionRouter())
    
    # ---- STAGE 8: Output ----
    # Print to console for monitoring
    routed_decisions.print()
    
    # Write to Kafka output topic
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(OUTPUT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()
    
    routed_decisions \
        .map(to_json) \
        .sink_to(kafka_sink)
    
    return routed_decisions


def main():
    """
    Main entry point for fraud detection pipeline.
    """
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Add Kafka connector JARs to classpath
    jars = [
        "file:///Users/anupdangi/flink-1.18.1/lib/flink-connector-kafka-3.0.2-1.18.jar",
        "file:///Users/anupdangi/flink-jars/kafka-clients-3.4.0.jar"
    ]
    env.add_jars(*jars)
    
    # Configure parallelism
    env.set_parallelism(1)  # Single parallelism for local testing
    
    # Enable checkpointing for fault tolerance
    env.enable_checkpointing(60000)  # Checkpoint every 60 seconds
    
    # Build and execute pipeline
    logger.info("Building fraud detection pipeline...")
    build_pipeline(env)
    
    # Display configuration
    logger.info("=" * 60)
    logger.info("🚀 Fraud Detection Pipeline Started")
    logger.info("=" * 60)
    logger.info(f"📥 Input Topic: {INPUT_TOPIC}")
    logger.info(f"📤 Output Topic: {OUTPUT_TOPIC}")
    logger.info(f"📊 Kafka Brokers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"👥 Consumer Group: {CONSUMER_GROUP}")
    logger.info(f"⚙️  Low Risk Threshold: {LOW_RISK_THRESHOLD}")
    logger.info(f"⚙️  High Risk Threshold: {HIGH_RISK_THRESHOLD}")
    logger.info(f"⚖️  Velocity Weight: {VELOCITY_WEIGHT}")
    logger.info(f"⚖️  Device Weight: {DEVICE_WEIGHT}")
    logger.info(f"⚖️  ML Weight: {ML_WEIGHT} (disabled)")
    logger.info(f"⏱️  Max Out-of-Orderness: {MAX_OUT_OF_ORDERNESS_SECONDS}s")
    logger.info(f"💾 Checkpointing: Every 60s")
    logger.info("=" * 60)
    
    logger.info("Starting Flink execution...")
    env.execute("Real-Time Fraud Detection Pipeline")


if __name__ == "__main__":
    main()

