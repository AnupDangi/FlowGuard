#!/usr/bin/env python3
"""Check DAG readiness against local analytics PostgreSQL."""

import os
import sys

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
    print("=" * 60)
    print("CHECKING POSTGRES ANALYTICS SETUP FOR DAG READINESS")
    print("=" * 60)
    try:
        conn = connect()
        cur = conn.cursor()
        print("Connected to analytics Postgres")

        required_tables = [
            "analytics.bronze_orders_raw",
            "analytics.bronze_clicks_raw",
            "analytics.silver_orders_clean",
            "analytics.silver_clicks_clean",
            "analytics.gold_daily_gmv_metrics",
            "analytics.gold_ads_attribution",
        ]
        print("\n--- TABLES ---")
        for t in required_tables:
            cur.execute(
                """
                SELECT EXISTS (
                  SELECT 1
                  FROM information_schema.tables
                  WHERE table_schema = split_part(%s, '.', 1)
                    AND table_name = split_part(%s, '.', 2)
                )
                """,
                (t, t),
            )
            exists = cur.fetchone()[0]
            print(f"{'OK' if exists else 'MISSING'} {t}")

        print("\n--- BRONZE DATA ---")
        cur.execute("SELECT COUNT(*) FROM analytics.bronze_orders_raw")
        orders = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM analytics.bronze_clicks_raw")
        clicks = cur.fetchone()[0]
        print(f"bronze_orders_raw: {orders}")
        print(f"bronze_clicks_raw: {clicks}")

        print("\n--- RECOMMENDATION ---")
        if orders == 0 and clicks == 0:
            print("No Bronze data yet. Start producers + bronze consumer first.")
        else:
            print("DAG environment is ready.")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"FAILED: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
