#!/usr/bin/env python3
"""
FlowGuard Real-Time Attribution Job (PyFlink)

Architecture (mirrors Zomato's approach):
  raw.clicks.v1  ──┐
                    ├──► KeyedCoProcessFunction ──► Redis INCR counters
  raw.orders.v1  ──┘   keyed by (user_id, item_id)
                        30-min event-time window

What it does:
  1. Consumes both Kafka topics simultaneously
  2. Keys by (user_id, item_id)
  3. For each click: store in Flink state with 30-min TTL
  4. For each order: check if there's a click in state within 30 min
     → If yes: attributed order → INCR ads:attributed_orders:{item_id}:{date}
  5. Always: INCR ads:clicks:{item_id}:{date}
             INCR ads:impressions:{item_id}:{date}
             INCR ads:orders:{item_id}:{date}

Redis key schema:
  ads:impressions:{item_id}:{YYYY-MM-DD}   → integer counter
  ads:clicks:{item_id}:{YYYY-MM-DD}        → integer counter
  ads:orders:{item_id}:{YYYY-MM-DD}        → integer counter
  ads:attributed_orders:{item_id}:{YYYY-MM-DD} → integer counter
  ads:revenue:{item_id}:{YYYY-MM-DD}       → float (stored as string)

Zomato insight: Use Redis INCR (incremental) instead of accumulating
full event lists in Flink state. This avoids state overload — the
exact problem Zomato solved in their blog post.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Optional

from dotenv import load_dotenv
load_dotenv("/app/.env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("flowguard.flink.attribution")

# ── Config ───────────────────────────────────────────────────────────────────
KAFKA_BROKERS       = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:9092")
REDIS_HOST          = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT          = int(os.environ.get("REDIS_PORT", "6379"))
ATTRIBUTION_WINDOW  = 30 * 60  # 30 minutes in seconds
CONSUMER_GROUP      = "flink-attribution-group"


# ── Redis helper ─────────────────────────────────────────────────────────────
def get_redis_client():
    import redis
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def redis_key(metric: str, item_id: str, date_str: str) -> str:
    return f"ads:{metric}:{item_id}:{date_str}"


def increment_counter(r, metric: str, item_id: str, date_str: str, amount=1):
    """Increment a Redis counter. Sets 25-hour TTL on first write."""
    key = redis_key(metric, item_id, date_str)
    pipe = r.pipeline()
    pipe.incrbyfloat(key, amount)
    pipe.expire(key, 25 * 3600)  # 25h TTL — survives until next day's job runs
    pipe.execute()


# ── PyFlink Job ──────────────────────────────────────────────────────────────
def run_flink_job():
    """
    PyFlink DataStream job for real-time attribution.

    Uses KeyedCoProcessFunction to process clicks and orders on the same
    (user_id, item_id) key. Click state is stored with 30-min TTL.
    """
    try:
        from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
        from pyflink.datastream.connectors.kafka import (
            KafkaSource, KafkaOffsetsInitializer
        )
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.common.watermark_strategy import WatermarkStrategy
        from pyflink.datastream.functions import KeyedCoProcessFunction, RuntimeContext
        from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor
        from pyflink.common.typeinfo import Types
        from pyflink.common import Duration
    except ImportError as e:
        logger.error(f"PyFlink not available: {e}")
        logger.info("Falling back to pure Python Kafka consumer mode...")
        run_python_fallback()
        return

    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Add Kafka Connector JAR
    jar_path = "file:///app/flink_jobs/flink-sql-connector-kafka-3.0.0-1.18.jar"
    env.add_jars(jar_path)
    
    env.set_parallelism(2)

    # Checkpointing: exactly-once, every 60 seconds
    env.enable_checkpointing(60_000, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(30_000)
    env.get_checkpoint_config().set_checkpoint_timeout(120_000)

    # ── Kafka Sources ────────────────────────────────────────────────────────
    clicks_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("raw.clicks.v1")
        .set_group_id(f"{CONSUMER_GROUP}-clicks")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    orders_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("raw.orders.v1")
        .set_group_id(f"{CONSUMER_GROUP}-orders")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(10)
    )

    clicks_stream = env.from_source(
        clicks_source, watermark_strategy, "Kafka clicks source"
    )
    orders_stream = env.from_source(
        orders_source, watermark_strategy, "Kafka orders source"
    )

    # ── Parse and key by (user_id, item_id) ─────────────────────────────────
    def parse_click(raw: str):
        try:
            e = json.loads(raw)
            uid = str(e.get("user_id", ""))
            iid = str(e.get("item_id", ""))
            ts  = e.get("timestamp", "")
            return (f"{uid}_{iid}", json.dumps({"type": "click", "user_id": uid,
                    "item_id": iid, "timestamp": ts, "event_type": e.get("event_type", "click")}))
        except Exception:
            return None

    def parse_order(raw: str):
        try:
            e = json.loads(raw)
            uid = str(e.get("user_id", ""))
            iid = str(e.get("item_id", ""))
            ts  = e.get("timestamp", "")
            price = float(e.get("price", 0))
            return (f"{uid}_{iid}", json.dumps({"type": "order", "user_id": uid,
                    "item_id": iid, "timestamp": ts, "price": price,
                    "order_id": e.get("order_id", "")}))
        except Exception:
            return None

    keyed_clicks = (
        clicks_stream
        .map(parse_click, output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
        .filter(lambda x: x is not None)
        .key_by(lambda x: x[0])
    )

    keyed_orders = (
        orders_stream
        .map(parse_order, output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
        .filter(lambda x: x is not None)
        .key_by(lambda x: x[0])
    )

    # ── Attribution CoProcessFunction ────────────────────────────────────────
    class AttributionFunction(KeyedCoProcessFunction):
        """
        Processes clicks and orders keyed by (user_id, item_id).

        State:
          - click_timestamps: list of recent click timestamps (within 30 min)

        On click: store timestamp in state, INCR clicks/impressions counter
        On order: check state for click within 30 min → INCR attributed_orders
                  always INCR orders counter
        """

        def open(self, runtime_context: RuntimeContext):
            # Store list of click timestamps for this (user_id, item_id) key
            self._click_state = runtime_context.get_list_state(
                ListStateDescriptor("click_timestamps", Types.STRING())
            )
            self._redis = get_redis_client()

        def process_element1(self, value, ctx):
            """Process a click event."""
            try:
                event = json.loads(value[1])
                item_id  = event["item_id"]
                ts_str   = event["timestamp"]
                evt_type = event.get("event_type", "click")
                date_str = ts_str[:10] if ts_str else datetime.now().strftime("%Y-%m-%d")

                # Store click timestamp in state
                self._click_state.add(ts_str)

                # INCR Redis counters
                if evt_type == "impression":
                    increment_counter(self._redis, "impressions", item_id, date_str)
                else:
                    increment_counter(self._redis, "clicks", item_id, date_str)

                logger.debug(f"Click processed: item={item_id} type={evt_type}")
            except Exception as e:
                logger.warning(f"Error processing click: {e}")

        def process_element2(self, value, ctx):
            """Process an order event."""
            try:
                event    = json.loads(value[1])
                item_id  = event["item_id"]
                ts_str   = event["timestamp"]
                price    = float(event.get("price", 0))
                date_str = ts_str[:10] if ts_str else datetime.now().strftime("%Y-%m-%d")

                # Parse order timestamp
                try:
                    order_ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except Exception:
                    order_ts = datetime.now()

                # Check for click within 30-min attribution window
                attributed = False
                valid_clicks = []
                for click_ts_str in self._click_state.get():
                    try:
                        click_ts = datetime.fromisoformat(
                            click_ts_str.replace("Z", "+00:00")
                        )
                        diff = (order_ts - click_ts).total_seconds()
                        if 0 <= diff <= ATTRIBUTION_WINDOW:
                            attributed = True
                        # Keep clicks that are still within window
                        if diff <= ATTRIBUTION_WINDOW:
                            valid_clicks.append(click_ts_str)
                    except Exception:
                        pass

                # Update state: remove expired clicks (older than 30 min)
                self._click_state.clear()
                for ts in valid_clicks:
                    self._click_state.add(ts)

                # INCR Redis counters
                increment_counter(self._redis, "orders", item_id, date_str)
                increment_counter(self._redis, "revenue", item_id, date_str, price)
                if attributed:
                    increment_counter(self._redis, "attributed_orders", item_id, date_str)
                    logger.info(f"Attribution: item={item_id} order attributed to click")

            except Exception as e:
                logger.warning(f"Error processing order: {e}")

    # Connect the two streams and apply attribution
    keyed_clicks.connect(keyed_orders).process(
        AttributionFunction(),
        output_type=Types.STRING()
    )

    logger.info("Submitting Flink attribution job...")
    env.execute("FlowGuard Real-Time Attribution")


# ── Pure Python fallback (no Flink cluster needed for local testing) ─────────
def run_python_fallback():
    """
    Fallback: pure Python Kafka consumer that does the same attribution logic.
    Useful for local testing without a full Flink cluster.
    Runs two threads — one per topic — and writes to Redis.
    """
    import threading
    from confluent_kafka import Consumer, KafkaError

    logger.info("Starting Python fallback attribution consumer...")
    r = get_redis_client()

    # In-memory click state: {(user_id, item_id): [timestamps]}
    import collections
    click_state = collections.defaultdict(list)
    state_lock  = threading.Lock()

    def consume_clicks():
        c = Consumer({
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": f"{CONSUMER_GROUP}-clicks-py",
            "auto.offset.reset": "latest",
        })
        c.subscribe(["raw.clicks.v1"])
        logger.info("Click consumer started")
        while True:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                event    = json.loads(msg.value().decode())
                uid      = str(event.get("user_id", ""))
                iid      = str(event.get("item_id", ""))
                ts_str   = event.get("timestamp", "")
                evt_type = event.get("event_type", "click")
                date_str = ts_str[:10] if ts_str else datetime.now().strftime("%Y-%m-%d")

                with state_lock:
                    click_state[(uid, iid)].append(ts_str)

                if evt_type == "impression":
                    increment_counter(r, "impressions", iid, date_str)
                else:
                    increment_counter(r, "clicks", iid, date_str)

                logger.info(f"[Click] item={iid} type={evt_type} date={date_str}")
            except Exception as e:
                logger.warning(f"Click parse error: {e}")

    def consume_orders():
        c = Consumer({
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": f"{CONSUMER_GROUP}-orders-py",
            "auto.offset.reset": "latest",
        })
        c.subscribe(["raw.orders.v1"])
        logger.info("Order consumer started")
        while True:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                event    = json.loads(msg.value().decode())
                uid      = str(event.get("user_id", ""))
                iid      = str(event.get("item_id", ""))
                ts_str   = event.get("timestamp", "")
                price    = float(event.get("price", 0))
                date_str = ts_str[:10] if ts_str else datetime.now().strftime("%Y-%m-%d")

                try:
                    order_ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except Exception:
                    order_ts = datetime.now()

                # Attribution check
                attributed = False
                with state_lock:
                    clicks = click_state.get((uid, iid), [])
                    valid  = []
                    for cts in clicks:
                        try:
                            ct   = datetime.fromisoformat(cts.replace("Z", "+00:00"))
                            diff = (order_ts - ct).total_seconds()
                            if 0 <= diff <= ATTRIBUTION_WINDOW:
                                attributed = True
                            if diff <= ATTRIBUTION_WINDOW:
                                valid.append(cts)
                        except Exception:
                            pass
                    click_state[(uid, iid)] = valid

                increment_counter(r, "orders", iid, date_str)
                increment_counter(r, "revenue", iid, date_str, price)
                if attributed:
                    increment_counter(r, "attributed_orders", iid, date_str)
                    logger.info(f"[Attribution] item={iid} ← click within 30 min")

                logger.info(f"[Order] item={iid} price={price} attributed={attributed}")
            except Exception as e:
                logger.warning(f"Order parse error: {e}")

    t1 = threading.Thread(target=consume_clicks,  daemon=True)
    t2 = threading.Thread(target=consume_orders, daemon=True)
    t1.start()
    t2.start()

    logger.info("Attribution running. Press Ctrl+C to stop.")
    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        logger.info("Shutting down attribution consumer.")


if __name__ == "__main__":
    import sys
    mode = os.environ.get("ATTRIBUTION_MODE", "auto")

    if mode == "python" or "--python" in sys.argv:
        logger.info("Running in Python fallback mode (no Flink cluster needed)")
        run_python_fallback()
    else:
        logger.info("Attempting PyFlink mode...")
        run_flink_job()
