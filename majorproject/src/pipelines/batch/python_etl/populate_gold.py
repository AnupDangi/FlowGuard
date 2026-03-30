#!/usr/bin/env python3
"""Populate Gold tables from analytics Silver tables (PostgreSQL)."""

import argparse
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_conn():
    return psycopg2.connect(
        host=os.getenv("ANALYTICS_POSTGRES_HOST", "localhost"),
        port=int(os.getenv("ANALYTICS_POSTGRES_PORT", "5434")),
        user=os.getenv("ANALYTICS_POSTGRES_USER", "flowguard"),
        password=os.getenv("ANALYTICS_POSTGRES_PASSWORD", ""),
        dbname=os.getenv("ANALYTICS_POSTGRES_DB", "flowguard_analytics"),
    )


def get_silver_dates(cur):
    cur.execute("SELECT DISTINCT date_partition FROM analytics.silver_orders_clean ORDER BY 1")
    return [str(r[0]) for r in cur.fetchall() if r[0] is not None]


def populate_gmv(cur, date_str: str):
    cur.execute("DELETE FROM analytics.gold_daily_gmv_metrics WHERE date = %s", (date_str,))
    cur.execute(
        """
        INSERT INTO analytics.gold_daily_gmv_metrics (
            date, total_revenue, avg_order_value, order_count, total_items_sold,
            unique_users, new_users, returning_users, revenue_growth_pct,
            order_growth_pct, load_timestamp, processing_version
        )
        SELECT
            %s::date AS date,
            COALESCE(SUM(price), 0) AS total_revenue,
            COALESCE(AVG(price), 0) AS avg_order_value,
            COUNT(order_id) AS order_count,
            COUNT(order_id) AS total_items_sold,
            COUNT(DISTINCT user_id) AS unique_users,
            0 AS new_users,
            0 AS returning_users,
            0 AS revenue_growth_pct,
            0 AS order_growth_pct,
            NOW() AS load_timestamp,
            'v1.0' AS processing_version
        FROM analytics.silver_orders_clean
        WHERE date_partition = %s
          AND is_valid = TRUE
        """,
        (date_str, date_str),
    )


def populate_ads(cur, date_str: str):
    cur.execute("DELETE FROM analytics.gold_ads_attribution WHERE date = %s", (date_str,))
    cur.execute(
        """
        INSERT INTO analytics.gold_ads_attribution (
            date, item_id, item_name, impression_count, click_count, order_count,
            ctr, conversion_rate, total_revenue, calculated_at
        )
        WITH impressions AS (
            SELECT item_id, COUNT(*) AS impression_count
            FROM analytics.silver_clicks_clean
            WHERE date_partition = %s
              AND is_valid = TRUE
              AND event_type = 'impression'
            GROUP BY item_id
        ),
        clicks AS (
            SELECT item_id, COUNT(*) AS click_count
            FROM analytics.silver_clicks_clean
            WHERE date_partition = %s
              AND is_valid = TRUE
              AND event_type = 'click'
            GROUP BY item_id
        ),
        orders AS (
            SELECT item_id, item_name, COUNT(order_id) AS order_count, COALESCE(SUM(price),0) AS total_revenue
            FROM analytics.silver_orders_clean
            WHERE date_partition = %s
              AND is_valid = TRUE
            GROUP BY item_id, item_name
        ),
        attributed AS (
            SELECT c.item_id, COUNT(DISTINCT o.order_id) AS attributed_orders
            FROM analytics.silver_clicks_clean c
            JOIN analytics.silver_orders_clean o
              ON c.user_id = o.user_id
             AND c.item_id = o.item_id
             AND o.order_timestamp >= c.click_timestamp
             AND o.order_timestamp <= c.click_timestamp + INTERVAL '30 minute'
            WHERE c.date_partition = %s
              AND c.is_valid = TRUE
              AND c.event_type = 'click'
              AND o.date_partition = %s
              AND o.is_valid = TRUE
            GROUP BY c.item_id
        )
        SELECT
            %s::date AS date,
            o.item_id,
            o.item_name,
            COALESCE(i.impression_count, 0) AS impression_count,
            COALESCE(cl.click_count, 0) AS click_count,
            o.order_count,
            CASE WHEN COALESCE(i.impression_count,0) > 0
                 THEN (COALESCE(cl.click_count,0)::numeric / i.impression_count::numeric)
                 ELSE 0 END AS ctr,
            CASE WHEN COALESCE(cl.click_count,0) > 0
                 THEN (COALESCE(a.attributed_orders,0)::numeric / cl.click_count::numeric)
                 ELSE 0 END AS conversion_rate,
            o.total_revenue,
            NOW() AS calculated_at
        FROM orders o
        LEFT JOIN impressions i ON o.item_id = i.item_id
        LEFT JOIN clicks cl ON o.item_id = cl.item_id
        LEFT JOIN attributed a ON o.item_id = a.item_id
        """,
        (date_str, date_str, date_str, date_str, date_str, date_str),
    )


def run(target_date: str | None):
    conn = get_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            dates = [target_date] if target_date else get_silver_dates(cur)
            for d in dates:
                populate_gmv(cur, d)
                populate_ads(cur, d)
            conn.commit()
            print(f"Gold populated for dates: {', '.join(dates) if dates else 'none'}")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate Gold from Silver")
    parser.add_argument("--date", help="Specific date YYYY-MM-DD (default: all dates)")
    args = parser.parse_args()
    run(args.date)
