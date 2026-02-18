#!/usr/bin/env python3
"""
FlowGuard Pipeline Verification Script

Checks the full pipeline end-to-end:
1. Snowflake connectivity
2. Bronze table schema (PARTITION_DATE column exists)
3. Row counts in Bronze, Silver, Gold
4. Sample data quality (no NULLs in key columns)
5. Runs schema setup SQL if tables are missing

Usage:
    python scripts/verify_pipeline.py
    python scripts/verify_pipeline.py --fix   # also runs schema setup SQL
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import date

# Load .env
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import snowflake.connector
from snowflake.connector.errors import ProgrammingError

ACCOUNT  = os.environ["SNOWFLAKE_ACCOUNT"]
USER     = os.environ["SNOWFLAKE_USER"]
PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "FLOWGUARD_DB")
WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

TODAY = date.today().isoformat()

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg):   print(f"  {GREEN}✅ {msg}{RESET}")
def fail(msg): print(f"  {RED}❌ {msg}{RESET}")
def warn(msg): print(f"  {YELLOW}⚠️  {msg}{RESET}")
def info(msg): print(f"  {CYAN}ℹ️  {msg}{RESET}")
def header(msg): print(f"\n{BOLD}{msg}{RESET}")


def connect():
    return snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
    )


def check_column_exists(cur, schema, table, column):
    cur.execute(f"""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME   = '{table}'
          AND COLUMN_NAME  = '{column}'
    """)
    return cur.fetchone()[0] > 0


def run_schema_setup(cur):
    """Run the schema setup SQL to add missing columns and create missing tables."""
    schema_sql = Path(__file__).parent / "setup_snowflake_schema.sql"
    if not schema_sql.exists():
        fail(f"Schema SQL not found: {schema_sql}")
        return False

    print(f"\n  Running {schema_sql.name}...")
    sql = schema_sql.read_text()

    # Split on semicolons, skip comments and empty statements
    statements = [
        s.strip() for s in sql.split(";")
        if s.strip() and not s.strip().startswith("--")
    ]

    errors = 0
    for stmt in statements:
        if not stmt:
            continue
        try:
            cur.execute(stmt)
            print(f"    {GREEN}OK{RESET}: {stmt[:60].replace(chr(10), ' ')}...")
        except ProgrammingError as e:
            # Ignore "already exists" errors
            if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                print(f"    {YELLOW}SKIP{RESET}: {stmt[:60].replace(chr(10), ' ')}... (already exists)")
            else:
                print(f"    {RED}ERR{RESET}: {e}")
                errors += 1

    return errors == 0


def main(fix: bool):
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  FlowGuard Pipeline Verification — {TODAY}{RESET}")
    print(f"{BOLD}{'='*60}{RESET}")

    # ── 1. Connectivity ──────────────────────────────────────────
    header("1. Snowflake Connectivity")
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION(), CURRENT_DATABASE(), CURRENT_WAREHOUSE()")
        row = cur.fetchone()
        ok(f"Connected — Snowflake {row[0]} | DB: {row[1]} | WH: {row[2]}")
    except Exception as e:
        fail(f"Cannot connect to Snowflake: {e}")
        sys.exit(1)

    # ── 2. Schema check ──────────────────────────────────────────
    header("2. Table Schema Check")

    checks = [
        ("BRONZE", "ORDERS_RAW",  "PARTITION_DATE"),
        ("BRONZE", "ORDERS_RAW",  "EVENT_TIMESTAMP"),
        ("BRONZE", "CLICKS_RAW",  "PARTITION_DATE"),
        ("BRONZE", "CLICKS_RAW",  "EVENT_TIMESTAMP"),
        ("SILVER", "ORDERS_CLEAN", "DATE_PARTITION"),
        ("SILVER", "CLICKS_CLEAN", "DATE_PARTITION"),
        ("GOLD",   "DAILY_GMV_METRICS", "DATE"),
        ("GOLD",   "ADS_ATTRIBUTION",   "DATE"),
    ]

    schema_ok = True
    for schema, table, col in checks:
        try:
            exists = check_column_exists(cur, schema, table, col)
            if exists:
                ok(f"{schema}.{table}.{col}")
            else:
                fail(f"{schema}.{table}.{col} — MISSING")
                schema_ok = False
        except ProgrammingError as e:
            fail(f"{schema}.{table} — table doesn't exist: {e}")
            schema_ok = False

    if not schema_ok:
        if fix:
            warn("Schema issues found. Running setup SQL...")
            if run_schema_setup(cur):
                ok("Schema setup complete")
            else:
                fail("Schema setup had errors — check output above")
        else:
            warn("Run with --fix to auto-apply schema setup SQL")

    # ── 3. Row counts ────────────────────────────────────────────
    header("3. Row Counts")

    tables = [
        ("BRONZE", "ORDERS_RAW",  f"PARTITION_DATE = '{TODAY}'"),
        ("BRONZE", "CLICKS_RAW",  f"PARTITION_DATE = '{TODAY}'"),
        ("SILVER", "ORDERS_CLEAN", f"DATE_PARTITION = '{TODAY}'"),
        ("SILVER", "CLICKS_CLEAN", f"DATE_PARTITION = '{TODAY}'"),
        ("GOLD",   "DAILY_GMV_METRICS", f"DATE = '{TODAY}'"),
        ("GOLD",   "ADS_ATTRIBUTION",   f"DATE = '{TODAY}'"),
    ]

    for schema, table, where in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table} WHERE {where}")
            count = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            total = cur.fetchone()[0]
            if count > 0:
                ok(f"{schema}.{table}: {count} rows today ({total} total)")
            elif total > 0:
                warn(f"{schema}.{table}: 0 rows today, {total} total (different date?)")
            else:
                warn(f"{schema}.{table}: EMPTY — no data yet")
        except ProgrammingError as e:
            fail(f"{schema}.{table}: {e}")

    # ── 4. Data quality check ────────────────────────────────────
    header("4. Data Quality — Bronze Sample (last 5 rows)")

    for schema, table, ts_col, id_col in [
        ("BRONZE", "ORDERS_RAW",  "EVENT_TIMESTAMP", "ORDER_ID"),
        ("BRONZE", "CLICKS_RAW",  "EVENT_TIMESTAMP", "EVENT_ID"),
    ]:
        try:
            cur.execute(f"""
                SELECT {id_col}, {ts_col}, PARTITION_DATE, INGESTION_ID
                FROM {schema}.{table}
                ORDER BY {ts_col} DESC NULLS LAST
                LIMIT 5
            """)
            rows = cur.fetchall()
            if not rows:
                warn(f"{schema}.{table}: no rows to check")
                continue

            null_ts = sum(1 for r in rows if r[1] is None)
            null_pd = sum(1 for r in rows if r[2] is None)
            null_id = sum(1 for r in rows if r[0] is None)

            if null_ts == 0 and null_pd == 0 and null_id == 0:
                ok(f"{schema}.{table}: all 5 sample rows have non-NULL timestamps ✓")
                for r in rows:
                    info(f"  {id_col}={str(r[0])[:8]}... ts={r[1]} date={r[2]}")
            else:
                fail(f"{schema}.{table}: NULL values found — ts_null={null_ts}, date_null={null_pd}, id_null={null_id}")
                for r in rows:
                    print(f"    {id_col}={r[0]} ts={r[1]} date={r[2]}")
        except ProgrammingError as e:
            fail(f"{schema}.{table}: {e}")

    # ── 5. Summary ───────────────────────────────────────────────
    header("5. Next Steps")
    print(f"""
  If Bronze rows = 0:
    → Start the web app and place some orders:
        cd zomato-web-app && pnpm dev
        Open http://localhost:3000, click food items, place an order

    → Make sure bronze consumer is running:
        ./scripts/start_bronze_consumer.sh &

  If Bronze has data but Silver/Gold are empty:
    → Run the Spark ETL:
        ./spark/run_etl.sh {TODAY}

  If you see NULL timestamps in Bronze:
    → The snowflake_writer.py fix is not deployed yet.
        Restart the bronze consumer: ./scripts/start_bronze_consumer.sh
""")

    cur.close()
    conn.close()
    print(f"{BOLD}{'='*60}{RESET}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify FlowGuard pipeline end-to-end")
    parser.add_argument("--fix", action="store_true", help="Auto-run schema setup SQL if issues found")
    args = parser.parse_args()
    main(fix=args.fix)
