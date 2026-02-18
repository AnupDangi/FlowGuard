#!/usr/bin/env python3
"""
FlowGuard Gold Layer Populator (Python-native, no Spark needed)

Reads Silver tables and writes Gold aggregates directly using the
Snowflake Python connector. This is for verification/small data.
For production scale, use spark/run_etl.sh instead.

Usage:
    python scripts/populate_gold.py                    # all dates in Silver
    python scripts/populate_gold.py --date 2026-02-16  # specific date
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import snowflake.connector

ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
USER      = os.environ["SNOWFLAKE_USER"]
PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
DATABASE  = os.environ.get("SNOWFLAKE_DATABASE", "FLOWGUARD_DB")
WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

GREEN = "\033[92m"; RED = "\033[91m"; YELLOW = "\033[93m"; RESET = "\033[0m"; BOLD = "\033[1m"
def ok(m):   print(f"  {GREEN}✅ {m}{RESET}")
def fail(m): print(f"  {RED}❌ {m}{RESET}")
def info(m): print(f"  {m}")


def populate_gmv(cur, date_str: str):
    """Aggregate SILVER.ORDERS_CLEAN → GOLD.DAILY_GMV_METRICS for a date."""
    print(f"\n  ▶ GMV metrics for {date_str}...")

    # Check Silver has data
    cur.execute(f"SELECT COUNT(*) FROM SILVER.ORDERS_CLEAN WHERE DATE_PARTITION = '{date_str}' AND IS_VALID = TRUE")
    count = cur.fetchone()[0]
    if count == 0:
        print(f"  ⚠️  No valid orders in Silver for {date_str} — skipping GMV")
        return

    # Delete existing Gold row for idempotency
    cur.execute(f"DELETE FROM GOLD.DAILY_GMV_METRICS WHERE DATE = '{date_str}'")

    # Insert aggregated metrics — matches actual table schema
    cur.execute(f"""
        INSERT INTO GOLD.DAILY_GMV_METRICS
            (DATE, TOTAL_REVENUE, AVG_ORDER_VALUE, ORDER_COUNT, TOTAL_ITEMS_SOLD,
             UNIQUE_USERS, NEW_USERS, RETURNING_USERS,
             REVENUE_GROWTH_PCT, ORDER_GROWTH_PCT,
             LOAD_TIMESTAMP, PROCESSING_VERSION)
        SELECT
            '{date_str}'::DATE          AS DATE,
            SUM(PRICE)                  AS TOTAL_REVENUE,
            AVG(PRICE)                  AS AVG_ORDER_VALUE,
            COUNT(ORDER_ID)             AS ORDER_COUNT,
            COUNT(ORDER_ID)             AS TOTAL_ITEMS_SOLD,   -- 1 item per order in current schema
            COUNT(DISTINCT USER_ID)     AS UNIQUE_USERS,
            0                           AS NEW_USERS,          -- requires user history table
            0                           AS RETURNING_USERS,
            0.0                         AS REVENUE_GROWTH_PCT, -- requires prior day data
            0.0                         AS ORDER_GROWTH_PCT,
            CURRENT_TIMESTAMP()         AS LOAD_TIMESTAMP,
            'v1.0'                      AS PROCESSING_VERSION
        FROM SILVER.ORDERS_CLEAN
        WHERE DATE_PARTITION = '{date_str}'
          AND IS_VALID = TRUE
    """)

    # Fetch and display result
    cur.execute(f"""
        SELECT TOTAL_REVENUE, ORDER_COUNT, UNIQUE_USERS, AVG_ORDER_VALUE
        FROM GOLD.DAILY_GMV_METRICS WHERE DATE = '{date_str}'
    """)
    row = cur.fetchone()
    ok(f"GMV [{date_str}]: Revenue=₹{row[0]:.2f}, Orders={row[1]}, Users={row[2]}, AOV=₹{row[3]:.2f}")


def populate_ads(cur, date_str: str):
    """Compute ads attribution SILVER → GOLD.ADS_ATTRIBUTION for a date."""
    print(f"\n  ▶ Ads attribution for {date_str}...")

    # Create ADS_ATTRIBUTION table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS GOLD.ADS_ATTRIBUTION (
            DATE              DATE,
            ITEM_ID           VARCHAR,
            ITEM_NAME         VARCHAR,
            IMPRESSION_COUNT  INTEGER,
            CLICK_COUNT       INTEGER,
            ORDER_COUNT       INTEGER,
            CTR               FLOAT,
            CONVERSION_RATE   FLOAT,
            TOTAL_REVENUE     FLOAT,
            CALCULATED_AT     TIMESTAMP_NTZ
        )
    """)

    cur.execute(f"SELECT COUNT(*) FROM SILVER.CLICKS_CLEAN WHERE DATE_PARTITION = '{date_str}' AND IS_VALID = TRUE")
    click_count = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM SILVER.ORDERS_CLEAN WHERE DATE_PARTITION = '{date_str}' AND IS_VALID = TRUE")
    order_count = cur.fetchone()[0]

    if click_count == 0 and order_count == 0:
        print(f"  ⚠️  No clicks or orders in Silver for {date_str} — skipping ads attribution")
        return

    # Delete existing for idempotency
    cur.execute(f"DELETE FROM GOLD.ADS_ATTRIBUTION WHERE DATE = '{date_str}'")

    # Attribution: click → order within 30 minutes, same user + item
    # Uses a CTE to join clicks and orders, then aggregates per item
    cur.execute(f"""
        INSERT INTO GOLD.ADS_ATTRIBUTION
            (DATE, ITEM_ID, ITEM_NAME, IMPRESSION_COUNT, CLICK_COUNT, ORDER_COUNT, CTR, CONVERSION_RATE, TOTAL_REVENUE, CALCULATED_AT)
        WITH
        impressions AS (
            SELECT ITEM_ID, COUNT(*) AS IMPRESSION_COUNT
            FROM SILVER.CLICKS_CLEAN
            WHERE DATE_PARTITION = '{date_str}' AND IS_VALID = TRUE AND EVENT_TYPE = 'impression'
            GROUP BY ITEM_ID
        ),
        clicks AS (
            SELECT ITEM_ID, COUNT(*) AS CLICK_COUNT
            FROM SILVER.CLICKS_CLEAN
            WHERE DATE_PARTITION = '{date_str}' AND IS_VALID = TRUE AND EVENT_TYPE = 'click'
            GROUP BY ITEM_ID
        ),
        orders AS (
            SELECT ITEM_ID, ITEM_NAME, COUNT(ORDER_ID) AS ORDER_COUNT, SUM(PRICE) AS TOTAL_REVENUE
            FROM SILVER.ORDERS_CLEAN
            WHERE DATE_PARTITION = '{date_str}' AND IS_VALID = TRUE
            GROUP BY ITEM_ID, ITEM_NAME
        ),
        attributed AS (
            -- 30-min attribution window: order within 30 min after a click, same user+item
            SELECT c.ITEM_ID, COUNT(DISTINCT o.ORDER_ID) AS ATTRIBUTED_ORDERS
            FROM SILVER.CLICKS_CLEAN c
            JOIN SILVER.ORDERS_CLEAN o
              ON  c.USER_ID = o.USER_ID
              AND c.ITEM_ID = o.ITEM_ID
              AND o.ORDER_TIMESTAMP >= c.CLICK_TIMESTAMP
              AND o.ORDER_TIMESTAMP <= DATEADD('minute', 30, c.CLICK_TIMESTAMP)
            WHERE c.DATE_PARTITION = '{date_str}' AND c.IS_VALID = TRUE
              AND o.DATE_PARTITION = '{date_str}' AND o.IS_VALID = TRUE
            GROUP BY c.ITEM_ID
        )
        SELECT
            '{date_str}'::DATE                                              AS DATE,
            o.ITEM_ID,
            o.ITEM_NAME,
            COALESCE(i.IMPRESSION_COUNT, 0)                                AS IMPRESSION_COUNT,
            COALESCE(cl.CLICK_COUNT, 0)                                    AS CLICK_COUNT,
            o.ORDER_COUNT,
            CASE WHEN COALESCE(i.IMPRESSION_COUNT,0) > 0
                 THEN COALESCE(cl.CLICK_COUNT,0) / i.IMPRESSION_COUNT
                 ELSE 0 END                                                AS CTR,
            CASE WHEN COALESCE(cl.CLICK_COUNT,0) > 0
                 THEN COALESCE(a.ATTRIBUTED_ORDERS,0) / cl.CLICK_COUNT
                 ELSE 0 END                                                AS CONVERSION_RATE,
            o.TOTAL_REVENUE,
            CURRENT_TIMESTAMP()                                            AS CALCULATED_AT
        FROM orders o
        LEFT JOIN impressions i  ON o.ITEM_ID = i.ITEM_ID
        LEFT JOIN clicks cl      ON o.ITEM_ID = cl.ITEM_ID
        LEFT JOIN attributed a   ON o.ITEM_ID = a.ITEM_ID
    """)

    cur.execute(f"SELECT COUNT(*) FROM GOLD.ADS_ATTRIBUTION WHERE DATE = '{date_str}'")
    n = cur.fetchone()[0]
    ok(f"Ads attribution [{date_str}]: {n} items written to GOLD.ADS_ATTRIBUTION")

    # Show top 5 by revenue
    cur.execute(f"""
        SELECT ITEM_NAME, IMPRESSION_COUNT, CLICK_COUNT, ORDER_COUNT,
               ROUND(CTR*100,1) AS CTR_PCT, ROUND(CONVERSION_RATE*100,1) AS CVR_PCT,
               ROUND(TOTAL_REVENUE,2) AS REVENUE
        FROM GOLD.ADS_ATTRIBUTION
        WHERE DATE = '{date_str}'
        ORDER BY TOTAL_REVENUE DESC
        LIMIT 5
    """)
    rows = cur.fetchall()
    if rows:
        print(f"\n  {'Item':<20} {'Impr':>6} {'Clicks':>6} {'Orders':>6} {'CTR%':>6} {'CVR%':>6} {'Revenue':>10}")
        print(f"  {'-'*65}")
        for r in rows:
            print(f"  {str(r[0]):<20} {r[1]:>6} {r[2]:>6} {r[3]:>6} {r[4]:>5}% {r[5]:>5}% ₹{r[6]:>8}")


def get_silver_dates(cur) -> list:
    """Get all distinct dates present in Silver tables."""
    cur.execute("""
        SELECT DISTINCT DATE_PARTITION FROM SILVER.ORDERS_CLEAN
        WHERE DATE_PARTITION IS NOT NULL
        ORDER BY DATE_PARTITION
    """)
    return [str(r[0]) for r in cur.fetchall()]


def main(date_str: str | None):
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  FlowGuard Gold Layer Populator{RESET}")
    print(f"{BOLD}{'='*60}{RESET}")

    conn = snowflake.connector.connect(
        user=USER, password=PASSWORD, account=ACCOUNT,
        warehouse=WAREHOUSE, database=DATABASE,
    )
    cur = conn.cursor()

    if date_str:
        dates = [date_str]
    else:
        dates = get_silver_dates(cur)
        if not dates:
            print(f"\n  ⚠️  No dates found in SILVER.ORDERS_CLEAN. Run the bronze consumer first.")
            sys.exit(0)
        print(f"\n  Found {len(dates)} date(s) in Silver: {', '.join(dates)}")

    for d in dates:
        print(f"\n{'─'*60}")
        populate_gmv(cur, d)
        populate_ads(cur, d)

    cur.close()
    conn.close()
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  Done. Verify in Snowflake:{RESET}")
    print(f"  SELECT * FROM GOLD.DAILY_GMV_METRICS ORDER BY DATE DESC;")
    print(f"  SELECT * FROM GOLD.ADS_ATTRIBUTION ORDER BY DATE DESC, TOTAL_REVENUE DESC;")
    print(f"{BOLD}{'='*60}{RESET}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate Gold tables from Silver (no Spark needed)")
    parser.add_argument("--date", help="Specific date (YYYY-MM-DD). Default: all dates in Silver.")
    args = parser.parse_args()
    main(args.date)
