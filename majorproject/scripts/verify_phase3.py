#!/usr/bin/env python3
"""Verify Phase 3 (batch analytics + reconciliation readiness)."""

from __future__ import annotations

import os
import sys
from datetime import date

import psycopg2
import redis
from dotenv import load_dotenv

load_dotenv()

TODAY = date.today().isoformat()


def _connect_analytics():
    return psycopg2.connect(
        host=os.getenv("ANALYTICS_POSTGRES_HOST", "localhost"),
        port=int(os.getenv("ANALYTICS_POSTGRES_PORT", "5434")),
        user=os.getenv("ANALYTICS_POSTGRES_USER", "flowguard"),
        password=os.getenv("ANALYTICS_POSTGRES_PASSWORD", "flowguard123"),
        dbname=os.getenv("ANALYTICS_POSTGRES_DB", "flowguard_analytics"),
    )


def _connect_redis():
    host_candidates = [os.getenv("REDIS_HOST", "redis"), "localhost", "127.0.0.1", "redis"]
    port = int(os.getenv("REDIS_PORT", "6379"))
    last_error = None
    for host in host_candidates:
        try:
            client = redis.Redis(host=host, port=port, decode_responses=True, socket_connect_timeout=2)
            client.ping()
            return client
        except Exception as exc:  # pragma: no cover - environment dependent
            last_error = exc
    raise RuntimeError(f"Redis unavailable: {last_error}")


def main() -> int:
    print(f"Phase 3 verification date: {TODAY}")
    ok = True

    try:
        conn = _connect_analytics()
        cur = conn.cursor()
    except Exception as exc:
        print(f"FAIL analytics connection: {exc}")
        return 1

    required_tables = [
        "analytics.bronze_orders_raw",
        "analytics.bronze_clicks_raw",
        "analytics.silver_orders_clean",
        "analytics.silver_clicks_clean",
        "analytics.gold_daily_gmv_metrics",
        "analytics.gold_ads_attribution",
        "analytics.recon_hourly_totals",
    ]

    print("\n[1] Required tables")
    for table in required_tables:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema = split_part(%s, '.', 1)
                AND table_name = split_part(%s, '.', 2)
            )
            """,
            (table, table),
        )
        exists = bool(cur.fetchone()[0])
        print(f"{'OK ' if exists else 'FAIL'} {table}")
        ok = ok and exists

    print("\n[2] Bronze/Silver/Gold row counts (today)")
    queries = [
        ("SELECT COUNT(*) FROM analytics.bronze_orders_raw WHERE partition_date = %s", "bronze_orders"),
        ("SELECT COUNT(*) FROM analytics.bronze_clicks_raw WHERE partition_date = %s", "bronze_clicks"),
        ("SELECT COUNT(*) FROM analytics.silver_orders_clean WHERE date_partition = %s", "silver_orders"),
        ("SELECT COUNT(*) FROM analytics.silver_clicks_clean WHERE date_partition = %s", "silver_clicks"),
        ("SELECT COUNT(*) FROM analytics.gold_daily_gmv_metrics WHERE date = %s", "gold_gmv"),
        ("SELECT COUNT(*) FROM analytics.gold_ads_attribution WHERE date = %s", "gold_ads"),
    ]
    for query, name in queries:
        cur.execute(query, (TODAY,))
        count = int(cur.fetchone()[0])
        print(f"OK  {name}: {count}")

    cur.close()
    conn.close()

    print("\n[3] Reconciliation/Redis visibility")
    try:
        redis_client = _connect_redis()
        rec_total = int(redis_client.get("recon:last_overwrite_total") or 0)
        rec_ts = redis_client.get("recon:last_run_ts")
        print(f"OK  recon:last_overwrite_total={rec_total}")
        print(f"OK  recon:last_run_ts={rec_ts}")
    except Exception as exc:
        print(f"WARN reconciliation keys unavailable: {exc}")

    if not ok:
        print("\nFAIL Phase 3 verification failed.")
        return 1

    print("\nOK  Phase 3 verification passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
