#!/usr/bin/env python3
"""Verify FlowGuard pipeline against local PostgreSQL analytics backend."""

import argparse
import os
from datetime import date

import psycopg2
from dotenv import load_dotenv

load_dotenv()
TODAY = date.today().isoformat()


def connect():
    return psycopg2.connect(
        host=os.getenv("ANALYTICS_POSTGRES_HOST", "localhost"),
        port=int(os.getenv("ANALYTICS_POSTGRES_PORT", "5434")),
        user=os.getenv("ANALYTICS_POSTGRES_USER", "flowguard"),
        password=os.getenv("ANALYTICS_POSTGRES_PASSWORD", ""),
        dbname=os.getenv("ANALYTICS_POSTGRES_DB", "flowguard_analytics"),
    )


def main(_fix: bool):
    conn = connect()
    cur = conn.cursor()
    print("=" * 60)
    print(f"FlowGuard Pipeline Verification (POSTGRES) - {TODAY}")
    print("=" * 60)

    checks = [
        "analytics.bronze_orders_raw",
        "analytics.bronze_clicks_raw",
        "analytics.silver_orders_clean",
        "analytics.silver_clicks_clean",
        "analytics.gold_daily_gmv_metrics",
        "analytics.gold_ads_attribution",
        "analytics.recon_hourly_totals",
    ]
    print("\n1) Table existence")
    for t in checks:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema = split_part(%s, '.', 1)
                AND table_name = split_part(%s, '.', 2)
            )
            """,
            (t, t),
        )
        print(f"{'OK' if cur.fetchone()[0] else 'MISSING'} {t}")

    print("\n2) Row counts")
    for q, label in [
        ("SELECT COUNT(*) FROM analytics.bronze_orders_raw WHERE partition_date = %s", "bronze_orders_today"),
        ("SELECT COUNT(*) FROM analytics.bronze_clicks_raw WHERE partition_date = %s", "bronze_clicks_today"),
        ("SELECT COUNT(*) FROM analytics.silver_orders_clean WHERE date_partition = %s", "silver_orders_today"),
        ("SELECT COUNT(*) FROM analytics.silver_clicks_clean WHERE date_partition = %s", "silver_clicks_today"),
        ("SELECT COUNT(*) FROM analytics.gold_daily_gmv_metrics WHERE date = %s", "gold_gmv_today"),
        ("SELECT COUNT(*) FROM analytics.gold_ads_attribution WHERE date = %s", "gold_ads_today"),
    ]:
        cur.execute(q, (TODAY,))
        print(f"{label}: {cur.fetchone()[0]}")

    print("\n3) Basic quality check (latest Bronze rows)")
    cur.execute(
        """
        SELECT order_id, event_timestamp, partition_date, ingestion_id
        FROM analytics.bronze_orders_raw
        ORDER BY ingestion_timestamp DESC
        LIMIT 5
        """
    )
    rows = cur.fetchall()
    if rows:
        nulls = sum(1 for r in rows if r[1] is None or r[2] is None or r[3] is None)
        print(f"latest_bronze_orders_rows={len(rows)} null_issues={nulls}")
    else:
        print("No bronze order rows yet.")

    cur.close()
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify FlowGuard pipeline (Postgres)")
    parser.add_argument("--fix", action="store_true", help="Reserved for compatibility")
    args = parser.parse_args()
    main(args.fix)
