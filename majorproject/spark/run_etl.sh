#!/usr/bin/env bash
# =============================================================================
# FlowGuard ETL Runner
#
# Runs all Spark ETL jobs in order for a given date.
# Logs each job's output to spark/logs/YYYY-MM-DD/
#
# Usage:
#   ./spark/run_etl.sh                    # Run for today
#   ./spark/run_etl.sh 2026-02-18         # Run for a specific date
#   ./spark/run_etl.sh 2026-02-15 2026-02-18  # Backfill a date range
#
# Environment variables (loaded from .env if present):
#   SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
#   SNOWFLAKE_DATABASE (default: FLOWGUARD_DB)
#   SNOWFLAKE_WAREHOUSE (default: COMPUTE_WH)
#   SPARK_HOME (optional, defaults to system spark-submit)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
JOBS_DIR="$SCRIPT_DIR/jobs"
LOGS_BASE="$SCRIPT_DIR/logs"

# ── Load .env ────────────────────────────────────────────────────────────────
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$PROJECT_ROOT/.env"
    set +a
fi

# ── Validate required env vars ───────────────────────────────────────────────
for var in SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_PASSWORD; do
    if [ -z "${!var:-}" ]; then
        echo "❌ ERROR: $var is not set. Check your .env file."
        exit 1
    fi
done

# ── Determine date range ─────────────────────────────────────────────────────
START_DATE="${1:-$(date +%Y-%m-%d)}"
END_DATE="${2:-$START_DATE}"

# ── Spark submit command ─────────────────────────────────────────────────────
SPARK_SUBMIT="${SPARK_HOME:-}/bin/spark-submit"
if ! command -v "$SPARK_SUBMIT" &>/dev/null; then
    # Try plain spark-submit from PATH
    SPARK_SUBMIT="spark-submit"
fi

if ! command -v "$SPARK_SUBMIT" &>/dev/null; then
    echo "❌ ERROR: spark-submit not found. Install Spark or set SPARK_HOME."
    exit 1
fi

# ── Helper: run a single Spark job ──────────────────────────────────────────
run_job() {
    local job_name="$1"
    local job_file="$2"
    local date_str="$3"
    local log_dir="$4"
    local log_file="$log_dir/${job_name}.log"

    echo ""
    echo "▶ [$date_str] Running: $job_name"
    echo "  Log: $log_file"

    local start_ts
    start_ts=$(date +%s)

    if "$SPARK_SUBMIT" \
        --master "local[*]" \
        "$job_file" \
        --date "$date_str" \
        > "$log_file" 2>&1; then
        local end_ts
        end_ts=$(date +%s)
        local elapsed=$((end_ts - start_ts))
        echo "  ✅ Done in ${elapsed}s"
        echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) SUCCESS $job_name $date_str ${elapsed}s" >> "$LOGS_BASE/schedule.log"
    else
        local end_ts
        end_ts=$(date +%s)
        local elapsed=$((end_ts - start_ts))
        echo "  ❌ FAILED after ${elapsed}s — see $log_file"
        echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) FAILED  $job_name $date_str ${elapsed}s" >> "$LOGS_BASE/schedule.log"
        # Print last 20 lines of log for quick debugging
        echo ""
        echo "  --- Last 20 lines of log ---"
        tail -20 "$log_file" | sed 's/^/  /'
        echo "  ----------------------------"
        return 1
    fi
}

# ── Main loop ────────────────────────────────────────────────────────────────
echo "============================================================"
echo "  FlowGuard ETL Runner"
echo "  Date range: $START_DATE → $END_DATE"
echo "  Spark: $SPARK_SUBMIT"
echo "============================================================"

# Generate list of dates in range
current="$START_DATE"
while [[ "$current" <= "$END_DATE" ]]; do
    LOG_DIR="$LOGS_BASE/$current"
    mkdir -p "$LOG_DIR"

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Processing date: $current"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # Write run metadata
    echo "started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$LOG_DIR/run_meta.txt"
    echo "date=$current" >> "$LOG_DIR/run_meta.txt"

    # Run jobs in dependency order:
    # 1. Bronze → Silver (can run in parallel, but sequential for simplicity/log clarity)
    run_job "bronze_to_silver_orders" "$JOBS_DIR/bronze_to_silver_orders.py" "$current" "$LOG_DIR"
    run_job "bronze_to_silver_clicks" "$JOBS_DIR/bronze_to_silver_clicks.py" "$current" "$LOG_DIR"

    # 2. Silver → Gold (depends on both Silver tables being ready)
    run_job "silver_to_gold_gmv"  "$JOBS_DIR/silver_to_gold_gmv.py"  "$current" "$LOG_DIR"
    run_job "silver_to_gold_ads"  "$JOBS_DIR/silver_to_gold_ads.py"  "$current" "$LOG_DIR"

    echo "completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$LOG_DIR/run_meta.txt"
    echo ""
    echo "  ✅ All jobs complete for $current"
    echo "  Logs: $LOG_DIR/"

    # Advance to next day
    current=$(date -j -v+1d -f "%Y-%m-%d" "$current" +%Y-%m-%d 2>/dev/null \
              || date -d "$current + 1 day" +%Y-%m-%d)
done

echo ""
echo "============================================================"
echo "  ETL run complete. Schedule log: $LOGS_BASE/schedule.log"
echo "============================================================"
