#!/usr/bin/env python3
"""Populate Silver tables from analytics Bronze tables (PostgreSQL)."""

import argparse
import os
from datetime import date

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


def get_bronze_dates(cur):
    cur.execute(
        """
        SELECT DISTINCT partition_date FROM analytics.bronze_orders_raw
        UNION
        SELECT DISTINCT partition_date FROM analytics.bronze_clicks_raw
        ORDER BY 1
        """
    )
    return [str(r[0]) for r in cur.fetchall() if r[0] is not None]


def populate_orders_silver(cur, date_str: str):
    cur.execute("DELETE FROM analytics.silver_orders_clean WHERE date_partition = %s", (date_str,))
    cur.execute(
        """
        INSERT INTO analytics.silver_orders_clean (
            order_id, event_id, user_id, item_id, item_name, price, status,
            order_timestamp, date_partition, enriched_category,
            enriched_description, enriched_preparation_time, is_duplicate,
            is_valid, validation_errors, ingestion_id, source_table,
            load_timestamp, processing_version
        )
        WITH ranked AS (
            SELECT
                order_id,
                event_id,
                user_id::text AS user_id,
                item_id::text AS item_id,
                item_name,
                price,
                status,
                event_timestamp AS order_timestamp,
                partition_date AS date_partition,
                ingestion_id,
                ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_timestamp ASC) AS rn
            FROM analytics.bronze_orders_raw
            WHERE partition_date = %s
              AND order_id IS NOT NULL
              AND event_timestamp IS NOT NULL
        )
        SELECT
            r.order_id,
            r.event_id,
            r.user_id,
            r.item_id,
            r.item_name,
            r.price,
            r.status,
            r.order_timestamp,
            r.date_partition,
            NULL AS enriched_category,
            NULL AS enriched_description,
            NULL AS enriched_preparation_time,
            (r.rn > 1) AS is_duplicate,
            (r.price > 0 AND r.user_id IS NOT NULL AND r.item_id IS NOT NULL) AS is_valid,
            CASE
              WHEN r.price <= 0 THEN 'price_invalid'
              WHEN r.user_id IS NULL THEN 'user_id_null'
              WHEN r.item_id IS NULL THEN 'item_id_null'
              ELSE NULL
            END AS validation_errors,
            r.ingestion_id,
            'analytics.bronze_orders_raw' AS source_table,
            NOW() AS load_timestamp,
            'v1.0' AS processing_version
        FROM ranked r
        """,
        (date_str,),
    )
    return cur.rowcount


def populate_clicks_silver(cur, date_str: str):
    cur.execute("DELETE FROM analytics.silver_clicks_clean WHERE date_partition = %s", (date_str,))
    cur.execute(
        """
        INSERT INTO analytics.silver_clicks_clean (
            event_id, user_id, session_id, item_id, event_type, click_timestamp,
            date_partition, enriched_item_name, enriched_category, is_duplicate,
            is_valid, validation_errors, ingestion_id, source_table, load_timestamp,
            processing_version
        )
        WITH ranked AS (
            SELECT
                event_id,
                user_id::text AS user_id,
                session_id,
                item_id::text AS item_id,
                event_type,
                event_timestamp AS click_timestamp,
                partition_date AS date_partition,
                ingestion_id,
                ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp ASC) AS rn
            FROM analytics.bronze_clicks_raw
            WHERE partition_date = %s
              AND event_id IS NOT NULL
              AND event_timestamp IS NOT NULL
        )
        SELECT
            r.event_id,
            r.user_id,
            r.session_id,
            r.item_id,
            r.event_type,
            r.click_timestamp,
            r.date_partition,
            NULL AS enriched_item_name,
            NULL AS enriched_category,
            (r.rn > 1) AS is_duplicate,
            (r.user_id IS NOT NULL AND r.event_type IN ('click', 'impression')) AS is_valid,
            CASE
                WHEN r.user_id IS NULL THEN 'user_id_null'
                WHEN r.event_type NOT IN ('click', 'impression') THEN 'unknown_event_type'
                ELSE NULL
            END AS validation_errors,
            r.ingestion_id,
            'analytics.bronze_clicks_raw' AS source_table,
            NOW() AS load_timestamp,
            'v1.0' AS processing_version
        FROM ranked r
        """,
        (date_str,),
    )
    return cur.rowcount


def run(target_date: str | None, all_dates: bool):
    conn = get_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            if target_date:
                dates = [target_date]
            elif all_dates:
                dates = get_bronze_dates(cur)
            else:
                dates = [date.today().strftime("%Y-%m-%d")]

            for d in dates:
                populate_orders_silver(cur, d)
                populate_clicks_silver(cur, d)
            conn.commit()
            print(f"Silver populated for dates: {', '.join(dates) if dates else 'none'}")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate Silver from Bronze")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", help="Specific date YYYY-MM-DD")
    group.add_argument("--all", action="store_true", help="Process all Bronze dates")
    args = parser.parse_args()
    run(args.date, args.all)
