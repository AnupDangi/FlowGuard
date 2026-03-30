#!/usr/bin/env python3
"""Realtime fraud signals from canonical behavior stream (Phase 2).

Consumes ``behavior.events.v1`` and detects:
- VELOCITY_FRAUD: more than N orders per user in a sliding time window
- HIGH_VALUE_FRAUD: single order amount above threshold (uses ``metadata.price``)

Alerts are written to:
- Kafka topic ``fraud.alerts.v1``
- Redis list ``fraud:alerts:recent`` (trimmed)
- Redis counter ``metrics:fraud_alerts_total``

Cooldown per (user, alert_type) reduces duplicate alerts.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, Producer

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import redis  # noqa: E402

from src.shared.kafka.config import get_kafka_config  # noqa: E402
from src.shared.kafka.topics import TopicName  # noqa: E402


def _parse_event_time(raw) -> datetime:
    s = str(raw or "")
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def main() -> None:
    bootstrap = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:19093,localhost:19094"
    )
    topic = os.getenv("BEHAVIOR_TOPIC", TopicName.BEHAVIOR_EVENTS.value)
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))

    velocity_threshold = int(os.getenv("FRAUD_VELOCITY_THRESHOLD", "3"))
    window_minutes = float(os.getenv("FRAUD_VELOCITY_WINDOW_MINUTES", "5"))
    window_ms = int(window_minutes * 60 * 1000)
    high_value = float(os.getenv("FRAUD_HIGH_VALUE_THRESHOLD", "1500"))
    cooldown_sec = int(os.getenv("FRAUD_ALERT_COOLDOWN_SECONDS", "300"))

    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    kcfg = get_kafka_config()
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "fraud-detection-v1",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    prod_cfg = kcfg.to_producer_config()
    prod_cfg["bootstrap.servers"] = bootstrap
    producer = Producer(prod_cfg)

    consumer.subscribe([topic])
    print(f"Fraud detection consuming {topic} → alerts on {TopicName.FRAUD_ALERTS.value}")

    def emit_alert(user_id: int, alert_type: str, severity: str, message: str, details: dict) -> None:
        cooldown_key = f"fraud:cooldown:{alert_type}:user:{user_id}"
        if not r.set(cooldown_key, "1", nx=True, ex=cooldown_sec):
            return
        now = datetime.now(timezone.utc).isoformat()
        payload = {
            "alert_type": alert_type,
            "severity": severity,
            "user_id": user_id,
            "message": message,
            "detected_at": now,
            "details": details,
        }
        raw = json.dumps(payload)
        producer.produce(
            TopicName.FRAUD_ALERTS.value,
            raw.encode("utf-8"),
            key=str(user_id),
        )
        producer.poll(0)
        r.lpush("fraud:alerts:recent", raw)
        r.ltrim("fraud:alerts:recent", 0, 199)
        r.incr("metrics:fraud_alerts_total")
        print(f"⚠️ {alert_type} user={user_id} {message}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                event = json.loads(msg.value().decode("utf-8"))
            except Exception:
                consumer.commit(message=msg, asynchronous=False)
                continue

            et = str(event.get("event_type", "")).lower()
            user_id = int(event.get("user_id") or 0)
            if user_id <= 0:
                consumer.commit(message=msg, asynchronous=False)
                continue

            meta = event.get("metadata") or {}
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = {}

            if et == "order":
                try:
                    ts = _parse_event_time(event.get("event_time"))
                except Exception:
                    ts = datetime.now(timezone.utc)
                ts_ms = int(ts.timestamp() * 1000)
                ev_id = str(event.get("event_id") or f"e_{ts_ms}")
                zkey = f"fraud:velocity:orders:user:{user_id}"
                r.zadd(zkey, {ev_id: ts_ms})
                r.zremrangebyscore(zkey, 0, ts_ms - window_ms)
                r.expire(zkey, int(window_minutes * 60) + 60)
                cnt = r.zcard(zkey)
                if cnt > velocity_threshold:
                    emit_alert(
                        user_id,
                        "VELOCITY_FRAUD",
                        "HIGH",
                        f"User {user_id} placed {cnt} orders in {window_minutes} minutes (threshold {velocity_threshold})",
                        {"order_count": cnt, "threshold": velocity_threshold, "window_minutes": window_minutes},
                    )

                price = float(meta.get("price") or 0)
                if price >= high_value:
                    emit_alert(
                        user_id,
                        "HIGH_VALUE_FRAUD",
                        "MEDIUM",
                        f"Order amount ₹{price} exceeds threshold ₹{high_value}",
                        {"amount": price, "threshold": high_value, "order_id": meta.get("order_id")},
                    )

            consumer.commit(message=msg, asynchronous=False)
    finally:
        producer.flush(10)
        consumer.close()


if __name__ == "__main__":
    main()
