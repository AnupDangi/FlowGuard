#!/usr/bin/env python3
"""FlowGuard pipeline monitor for local PostgreSQL analytics stack."""

import json
import os
import subprocess
from datetime import datetime
from pathlib import Path

import psycopg2
import redis
from confluent_kafka import Consumer, TopicPartition
from dotenv import load_dotenv

load_dotenv()

project_root = Path(__file__).parent.parent


def connect_pg():
    return psycopg2.connect(
        host=os.getenv("ANALYTICS_POSTGRES_HOST", "localhost"),
        port=int(os.getenv("ANALYTICS_POSTGRES_PORT", "5434")),
        user=os.getenv("ANALYTICS_POSTGRES_USER", "flowguard"),
        password=os.getenv("ANALYTICS_POSTGRES_PASSWORD", ""),
        dbname=os.getenv("ANALYTICS_POSTGRES_DB", "flowguard_analytics"),
    )


def check_kafka():
    print("\n=== KAFKA STATUS ===")
    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:19093,localhost:19094"
            ),
            "group.id": os.getenv("CONSUMER_GROUP_ID", "bronze-consumer-group"),
            "auto.offset.reset": "earliest",
        }
    )
    stats = {}
    for topic in ["raw.orders.v1", "raw.clicks.v1"]:
        md = consumer.list_topics(topic, timeout=5)
        if topic not in md.topics:
            print(f"MISSING topic {topic}")
            continue
        total_lag = 0
        total_messages = 0
        for p in md.topics[topic].partitions:
            tp = TopicPartition(topic, p)
            low, high = consumer.get_watermark_offsets(tp, timeout=5)
            committed = consumer.committed([tp], timeout=5)[0].offset
            committed = 0 if committed < 0 else committed
            total_lag += high - committed
            total_messages += high - low
        stats[topic] = {"total_messages": total_messages, "consumer_lag": total_lag}
        print(f"{topic}: messages={total_messages}, lag={total_lag}")
    consumer.close()
    return stats


def check_postgres():
    print("\n=== POSTGRES ANALYTICS STATUS ===")
    conn = connect_pg()
    cur = conn.cursor()
    result = {}
    cur.execute("SELECT COUNT(*), MAX(ingestion_timestamp) FROM analytics.bronze_orders_raw")
    b_orders, b_orders_latest = cur.fetchone()
    cur.execute("SELECT COUNT(*), MAX(ingestion_timestamp) FROM analytics.bronze_clicks_raw")
    b_clicks, b_clicks_latest = cur.fetchone()
    cur.execute("SELECT COUNT(*) FROM analytics.silver_orders_clean")
    s_orders = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM analytics.silver_clicks_clean")
    s_clicks = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM analytics.gold_daily_gmv_metrics")
    g_rows = cur.fetchone()[0]
    print(f"Bronze orders={b_orders}, clicks={b_clicks}")
    print(f"Silver orders={s_orders}, clicks={s_clicks}")
    print(f"Gold rows={g_rows}")
    result["bronze"] = {"orders": b_orders, "clicks": b_clicks, "latest_orders_ingestion": str(b_orders_latest), "latest_clicks_ingestion": str(b_clicks_latest)}
    result["silver"] = {"orders": s_orders, "clicks": s_clicks}
    result["gold"] = {"daily_gmv_rows": g_rows}
    cur.close()
    conn.close()
    return result


def check_consumer():
    print("\n=== BRONZE CONSUMER STATUS ===")
    ps = subprocess.run(["ps", "aux"], capture_output=True, text=True)
    running = "src.consumers.bronze_consumer.main" in ps.stdout
    print("running" if running else "not running")
    return running


def check_airflow():
    print("\n=== AIRFLOW STATUS ===")
    try:
        out = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True, timeout=5
        )
        running = "flowguard-airflow-scheduler" in out.stdout
        print("running" if running else "not running")
        return running
    except Exception:
        print("unknown")
        return False


def check_personalization():
    print("\n=== PERSONALIZATION STATUS ===")
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    try:
        r = redis.Redis(host=host, port=port, decode_responses=True, socket_connect_timeout=2)
        if not r.ping():
            print("redis unavailable")
            return {"redis": "unavailable"}
        updates = int(r.get("metrics:personalization_profile_updates") or 0)
        category_keys = len(r.keys("ads:user:*:top_categories"))
        item_keys = len(r.keys("ads:user:*:top_items"))
        print(f"profile_updates={updates}, category_profiles={category_keys}, item_profiles={item_keys}")
        return {
            "redis": "healthy",
            "profile_updates": updates,
            "category_profiles": category_keys,
            "item_profiles": item_keys,
        }
    except Exception as e:
        print(f"error: {e}")
        return {"redis": "error", "error": str(e)}


def check_fraud():
    print("\n=== FRAUD DETECTION STATUS ===")
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    try:
        r = redis.Redis(host=host, port=port, decode_responses=True, socket_connect_timeout=2)
        if not r.ping():
            print("redis unavailable")
            return {"redis": "unavailable"}
        total = int(r.get("metrics:fraud_alerts_total") or 0)
        recent = r.llen("fraud:alerts:recent")
        print(f"fraud_alerts_total={total}, recent_list_len={recent}")
        return {"redis": "healthy", "fraud_alerts_total": total, "recent_alerts_len": recent}
    except Exception as e:
        print(f"error: {e}")
        return {"redis": "error", "error": str(e)}


def main():
    report = {"timestamp": datetime.now().isoformat()}
    report["kafka"] = check_kafka()
    report["analytics_db"] = check_postgres()
    report["consumer_running"] = check_consumer()
    report["airflow_running"] = check_airflow()
    report["personalization"] = check_personalization()
    report["fraud"] = check_fraud()

    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)
    path = logs_dir / f"pipeline_monitor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    path.write_text(json.dumps(report, indent=2))
    print(f"\nReport saved: {path}")


if __name__ == "__main__":
    main()
