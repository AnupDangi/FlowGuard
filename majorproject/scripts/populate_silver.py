#!/usr/bin/env python3
"""
FlowGuard Silver Layer Populator (Python-native, no Spark needed)

Reads Bronze tables and writes cleaned Silver data directly using
Snowflake SQL pushdown. Idempotent — safe to re-run for any date.

Usage:
    python scripts/populate_silver.py                    # today
    python scripts/populate_silver.py --date 2026-02-18  # specific date
    python scripts/populate_silver.py --all              # all Bronze dates
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import date, datetime

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import snowflake.connector

ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
USER      = os.environ["SNOWFLAKE_USER"]
PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
DATABASE  = os.environ.get("SNOWFLAKE_DATABASE", "FLOWGUARD_DB")
WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

GREEN = "\033[92m"; RED = "\033[91m"; YELLOW = "\033[93m"
RESET = "\033[0m"; BOLD = "\033[1m"
def ok(m):   print(f"  {GREEN}✅ {m}{RESET}")
def warn(m): print(f"  {YELLOW}⚠️  {m}{RESET}")
def fail(m): print(f"  {RED}❌ {m}{RESET}")
def info(m): print(f"  {m}")


def populate_orders_silver(cur, date_str: str) -> int:
    """Transform BRONZE.ORDERS_RAW → SILVER.ORDERS_CLEAN for a date.

    Actual SILVER.ORDERS_CLEAN columns:
      ORDER_ID, EVENT_ID, USER_ID, ITEM_ID, ITEM_NAME, PRICE, STATUS,
      ORDER_TIMESTAMP, DATE_PARTITION, ENRICHED_CATEGORY, ENRICHED_DESCRIPTION,
      ENRICHED_PREPARATION_TIME, IS_DUPLICATE, IS_VALID, VALIDATION_ERRORS,
      INGESTION_ID, SOURCE_TABLE, LOAD_TIMESTAMP, PROCESSING_VERSION
    """
    print(f"\n  ▶ Orders Silver for {date_str}...")

    # Check Bronze has data for this date
    cur.execute(f"""
        SELECT COUNT(*) FROM BRONZE.ORDERS_RAW
        WHERE PARTITION_DATE = '{date_str}'
    """)
    bronze_count = cur.fetchone()[0]
    if bronze_count == 0:
        warn(f"No Bronze orders for {date_str} — skipping")
        return 0

    # Delete existing Silver rows for idempotency
    cur.execute(f"DELETE FROM SILVER.ORDERS_CLEAN WHERE DATE_PARTITION = '{date_str}'")
    deleted = cur.rowcount
    if deleted > 0:
        info(f"Deleted {deleted} existing Silver order rows for {date_str}")

    # Insert: Bronze → Silver with dedup + validation
    # USER_ID and ITEM_ID are NUMBER in Bronze but VARCHAR in Silver — cast via TO_VARCHAR
    cur.execute(f"""
        INSERT INTO SILVER.ORDERS_CLEAN (
            ORDER_ID, EVENT_ID, USER_ID, ITEM_ID, ITEM_NAME, PRICE, STATUS,
            ORDER_TIMESTAMP, DATE_PARTITION,
            ENRICHED_CATEGORY, ENRICHED_DESCRIPTION, ENRICHED_PREPARATION_TIME,
            IS_DUPLICATE, IS_VALID, VALIDATION_ERRORS,
            INGESTION_ID, SOURCE_TABLE, LOAD_TIMESTAMP, PROCESSING_VERSION
        )
        WITH ranked AS (
            -- Deduplicate: keep first occurrence of each ORDER_ID
            SELECT
                ORDER_ID,
                EVENT_ID,
                TO_VARCHAR(USER_ID)         AS USER_ID,
                TO_VARCHAR(ITEM_ID)         AS ITEM_ID,
                ITEM_NAME,
                PRICE,
                STATUS,
                EVENT_TIMESTAMP             AS ORDER_TIMESTAMP,
                PARTITION_DATE              AS DATE_PARTITION,
                INGESTION_ID,
                ROW_NUMBER() OVER (
                    PARTITION BY ORDER_ID
                    ORDER BY EVENT_TIMESTAMP ASC
                )                           AS rn
            FROM BRONZE.ORDERS_RAW
            WHERE PARTITION_DATE = '{date_str}'
              AND ORDER_ID IS NOT NULL
              AND EVENT_TIMESTAMP IS NOT NULL
        )
        SELECT
            r.ORDER_ID,
            r.EVENT_ID,
            r.USER_ID,
            r.ITEM_ID,
            r.ITEM_NAME,
            r.PRICE,
            r.STATUS,
            r.ORDER_TIMESTAMP,
            r.DATE_PARTITION,
            -- Enrichment placeholders (would join a food catalog in production)
            NULL                            AS ENRICHED_CATEGORY,
            NULL                            AS ENRICHED_DESCRIPTION,
            NULL                            AS ENRICHED_PREPARATION_TIME,
            -- Dedup flag
            (r.rn > 1)                      AS IS_DUPLICATE,
            -- Validation: price must be > 0, user_id must be non-null
            (r.PRICE > 0 AND r.USER_ID IS NOT NULL AND r.ITEM_ID IS NOT NULL) AS IS_VALID,
            CASE
                WHEN r.PRICE <= 0 THEN 'price_invalid'
                WHEN r.USER_ID IS NULL THEN 'user_id_null'
                WHEN r.ITEM_ID IS NULL THEN 'item_id_null'
                ELSE NULL
            END                             AS VALIDATION_ERRORS,
            r.INGESTION_ID,
            'BRONZE.ORDERS_RAW'             AS SOURCE_TABLE,
            CURRENT_TIMESTAMP()             AS LOAD_TIMESTAMP,
            'v1.0'                          AS PROCESSING_VERSION
        FROM ranked r
    """)
    inserted = cur.rowcount

    # Count valid vs invalid
    cur.execute(f"""
        SELECT
            COUNT_IF(IS_VALID = TRUE)     AS valid_count,
            COUNT_IF(IS_DUPLICATE = TRUE) AS dup_count,
            COUNT_IF(IS_VALID = FALSE)    AS invalid_count
        FROM SILVER.ORDERS_CLEAN
        WHERE DATE_PARTITION = '{date_str}'
    """)
    row = cur.fetchone()
    ok(f"Orders Silver [{date_str}]: {inserted} rows inserted "
       f"(valid={row[0]}, duplicates={row[1]}, invalid={row[2]})")
    return inserted


def populate_clicks_silver(cur, date_str: str) -> int:
    """Transform BRONZE.CLICKS_RAW → SILVER.CLICKS_CLEAN for a date.

    Actual SILVER.CLICKS_CLEAN columns:
      EVENT_ID, USER_ID, SESSION_ID, ITEM_ID, EVENT_TYPE, CLICK_TIMESTAMP,
      DATE_PARTITION, ENRICHED_ITEM_NAME, ENRICHED_CATEGORY,
      IS_DUPLICATE, IS_VALID, VALIDATION_ERRORS,
      INGESTION_ID, SOURCE_TABLE, LOAD_TIMESTAMP, PROCESSING_VERSION
    """
    print(f"\n  ▶ Clicks Silver for {date_str}...")

    cur.execute(f"""
        SELECT COUNT(*) FROM BRONZE.CLICKS_RAW
        WHERE PARTITION_DATE = '{date_str}'
    """)
    bronze_count = cur.fetchone()[0]
    if bronze_count == 0:
        warn(f"No Bronze clicks for {date_str} — skipping")
        return 0

    cur.execute(f"DELETE FROM SILVER.CLICKS_CLEAN WHERE DATE_PARTITION = '{date_str}'")
    deleted = cur.rowcount
    if deleted > 0:
        info(f"Deleted {deleted} existing Silver click rows for {date_str}")

    cur.execute(f"""
        INSERT INTO SILVER.CLICKS_CLEAN (
            EVENT_ID, USER_ID, SESSION_ID, ITEM_ID, EVENT_TYPE, CLICK_TIMESTAMP,
            DATE_PARTITION, ENRICHED_ITEM_NAME, ENRICHED_CATEGORY,
            IS_DUPLICATE, IS_VALID, VALIDATION_ERRORS,
            INGESTION_ID, SOURCE_TABLE, LOAD_TIMESTAMP, PROCESSING_VERSION
        )
        WITH ranked AS (
            SELECT
                EVENT_ID,
                TO_VARCHAR(USER_ID)         AS USER_ID,
                NULL                        AS SESSION_ID,
                TO_VARCHAR(ITEM_ID)         AS ITEM_ID,
                EVENT_TYPE,
                EVENT_TIMESTAMP             AS CLICK_TIMESTAMP,
                PARTITION_DATE              AS DATE_PARTITION,
                INGESTION_ID,
                ROW_NUMBER() OVER (
                    PARTITION BY EVENT_ID
                    ORDER BY EVENT_TIMESTAMP ASC
                )                           AS rn
            FROM BRONZE.CLICKS_RAW
            WHERE PARTITION_DATE = '{date_str}'
              AND EVENT_ID IS NOT NULL
              AND EVENT_TIMESTAMP IS NOT NULL
        )
        SELECT
            r.EVENT_ID,
            r.USER_ID,
            r.SESSION_ID,
            r.ITEM_ID,
            r.EVENT_TYPE,
            r.CLICK_TIMESTAMP,
            r.DATE_PARTITION,
            -- Enrichment placeholders
            NULL                            AS ENRICHED_ITEM_NAME,
            NULL                            AS ENRICHED_CATEGORY,
            -- Dedup + validation
            (r.rn > 1)                      AS IS_DUPLICATE,
            (r.USER_ID IS NOT NULL AND r.EVENT_TYPE IN ('click', 'impression')) AS IS_VALID,
            CASE
                WHEN r.USER_ID IS NULL THEN 'user_id_null'
                WHEN r.EVENT_TYPE NOT IN ('click', 'impression') THEN 'unknown_event_type'
                ELSE NULL
            END                             AS VALIDATION_ERRORS,
            r.INGESTION_ID,
            'BRONZE.CLICKS_RAW'             AS SOURCE_TABLE,
            CURRENT_TIMESTAMP()             AS LOAD_TIMESTAMP,
            'v1.0'                          AS PROCESSING_VERSION
        FROM ranked r
    """)
    inserted = cur.rowcount

    cur.execute(f"""
        SELECT
            COUNT_IF(IS_VALID = TRUE AND EVENT_TYPE = 'click')      AS clicks,
            COUNT_IF(IS_VALID = TRUE AND EVENT_TYPE = 'impression') AS impressions,
            COUNT_IF(IS_DUPLICATE = TRUE)                           AS dups,
            COUNT_IF(IS_VALID = FALSE)                              AS invalid
        FROM SILVER.CLICKS_CLEAN
        WHERE DATE_PARTITION = '{date_str}'
    """)
    row = cur.fetchone()
    ok(f"Clicks Silver [{date_str}]: {inserted} rows inserted "
       f"(clicks={row[0]}, impressions={row[1]}, duplicates={row[2]}, invalid={row[3]})")
    return inserted


def get_bronze_dates(cur) -> list:
    """Get all distinct PARTITION_DATEs in Bronze tables."""
    cur.execute("""
        SELECT DISTINCT PARTITION_DATE FROM BRONZE.ORDERS_RAW
        WHERE PARTITION_DATE IS NOT NULL
        UNION
        SELECT DISTINCT PARTITION_DATE FROM BRONZE.CLICKS_RAW
        WHERE PARTITION_DATE IS NOT NULL
        ORDER BY 1
    """)
    return [str(r[0]) for r in cur.fetchall()]


def run(date_str: str | None, all_dates: bool):
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  FlowGuard Silver Layer Populator{RESET}")
    print(f"{BOLD}{'='*60}{RESET}")

    conn = snowflake.connector.connect(
        user=USER, password=PASSWORD, account=ACCOUNT,
        warehouse=WAREHOUSE, database=DATABASE,
    )
    cur = conn.cursor()

    if date_str:
        dates = [date_str]
    elif all_dates:
        dates = get_bronze_dates(cur)
        if not dates:
            warn("No dates found in Bronze tables. Run the bronze consumer first.")
            sys.exit(0)
        print(f"\n  Found {len(dates)} date(s) in Bronze: {', '.join(dates)}")
    else:
        # Default: today
        dates = [date.today().strftime("%Y-%m-%d")]

    total_orders = 0
    total_clicks = 0
    for d in dates:
        print(f"\n{'─'*60}")
        total_orders += populate_orders_silver(cur, d)
        total_clicks += populate_clicks_silver(cur, d)

    cur.close()
    conn.close()

    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  Done.{RESET}")
    print(f"  Total orders written to Silver: {total_orders}")
    print(f"  Total clicks written to Silver: {total_clicks}")
    print(f"\n  Verify in Snowflake:")
    print(f"  SELECT COUNT(*), DATE_PARTITION FROM SILVER.ORDERS_CLEAN GROUP BY 2 ORDER BY 2;")
    print(f"  SELECT COUNT(*), DATE_PARTITION FROM SILVER.CLICKS_CLEAN GROUP BY 2 ORDER BY 2;")
    print(f"{BOLD}{'='*60}{RESET}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Populate Silver tables from Bronze (no Spark needed)"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", help="Specific date YYYY-MM-DD (default: today)")
    group.add_argument("--all",  action="store_true", help="Process all Bronze dates")
    args = parser.parse_args()
    run(args.date, args.all)
