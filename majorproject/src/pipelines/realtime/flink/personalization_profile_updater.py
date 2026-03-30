#!/usr/bin/env python3
"""Rule-based realtime personalization profile updater.

Consumes canonical behavior events and updates Redis sorted sets:
- ads:user:{user_id}:top_categories
- ads:user:{user_id}:top_items
"""

import json
import math
import os
from datetime import datetime, timezone

import redis
from confluent_kafka import Consumer

WEIGHTS = {
    "order": 5.0,
    "click": 3.0,
    "view": 1.5,
    "impression": 1.0,
}


def parse_dt(value: str) -> datetime:
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def score_event(event: dict) -> float:
    event_type = str(event.get("event_type", "view"))
    base = WEIGHTS.get(event_type, 1.0)
    ts_raw = event.get("event_time")
    if not ts_raw:
        return base
    age_minutes = max(
        0.0,
        (datetime.now(timezone.utc) - parse_dt(str(ts_raw))).total_seconds() / 60.0,
    )
    decay = math.exp(-age_minutes / 240.0)
    return base * decay


def main():
    bootstrap = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:19093,localhost:19094"
    )
    topic = os.getenv("BEHAVIOR_TOPIC", "behavior.events.v1")
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    ttl_seconds = int(os.getenv("PERSONALIZATION_TTL_SECONDS", "604800"))

    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "personalization-updater-v1",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    print(f"Consuming {topic} for personalization updates...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
                user_id = int(event.get("user_id", 0))
                item_id = int(event.get("item_id", 0))
                category = str(event.get("category", "")).strip()
                if user_id <= 0:
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                score = score_event(event)
                item_key = f"ads:user:{user_id}:top_items"
                category_key = f"ads:user:{user_id}:top_categories"
                pipe = r.pipeline()
                if item_id > 0:
                    pipe.zincrby(item_key, score, item_id)
                    pipe.expire(item_key, ttl_seconds)
                if category:
                    pipe.zincrby(category_key, score, category)
                    pipe.expire(category_key, ttl_seconds)
                pipe.incr("metrics:personalization_profile_updates")
                pipe.execute()
                consumer.commit(message=msg, asynchronous=False)
            except Exception:
                consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
