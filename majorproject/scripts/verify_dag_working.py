#!/usr/bin/env python3
"""Verify FlowGuard Bronze/Silver/Gold in local analytics PostgreSQL."""

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def connect():
    return psycopg2.connect(
        host=os.getenv("ANALYTICS_POSTGRES_HOST", "localhost"),
        port=int(os.getenv("ANALYTICS_POSTGRES_PORT", "5434")),
        user=os.getenv("ANALYTICS_POSTGRES_USER", "flowguard"),
        password=os.getenv("ANALYTICS_POSTGRES_PASSWORD", ""),
        dbname=os.getenv("ANALYTICS_POSTGRES_DB", "flowguard_analytics"),
    )


def main():
    conn = connect()
    cur = conn.cursor()

    print("=" * 60)
    print("FlowGuard DATA PIPELINE STATUS (POSTGRES)")
    print("=" * 60)

    cur.execute("SELECT COUNT(*) FROM analytics.bronze_orders_raw")
    bronze_orders = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM analytics.bronze_clicks_raw")
    bronze_clicks = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM analytics.silver_orders_clean")
    silver_orders = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM analytics.silver_clicks_clean")
    silver_clicks = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM analytics.gold_daily_gmv_metrics")
    gold_gmv = cur.fetchone()[0]

    print(f"\nBRONZE: orders={bronze_orders}, clicks={bronze_clicks}")
    print(f"SILVER: orders={silver_orders}, clicks={silver_clicks}")
    print(f"GOLD: daily_gmv_rows={gold_gmv}")

    print("\n" + "=" * 60)
    if silver_orders > 0:
        print("DAG/ETL path is working (Bronze -> Silver at minimum).")
    else:
        print("Silver is empty. Trigger Silver ETL DAG and re-check.")
    print("=" * 60)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
