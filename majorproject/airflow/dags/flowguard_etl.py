"""
FlowGuard ETL DAGs

Two DAGs:
  1. flowguard_silver_etl  — runs hourly, Bronze → Silver
  2. flowguard_gold_etl    — runs daily at midnight, Silver → Gold

Both use PythonOperator calling the existing populate_*.py scripts.
Idempotent: safe to re-trigger for any date.
"""

from __future__ import annotations

import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ── Project root is mounted at /opt/airflow/project ─────────────────────────
PROJECT_ROOT = "/opt/airflow/project"
SCRIPTS_DIR  = f"{PROJECT_ROOT}/scripts"
VENV_PYTHON  = "/opt/airflow/venv/bin/python"

# Add project to path so we can import scripts directly
sys.path.insert(0, PROJECT_ROOT)

# ── Default args shared by both DAGs ────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "flowguard",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


# ═══════════════════════════════════════════════════════════════════════════
# DAG 1: Silver ETL — runs hourly
# Transforms Bronze → Silver for today's date
# ═══════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id="flowguard_silver_etl",
    description="Bronze → Silver transformation (hourly)",
    schedule_interval="0 * * * *",   # every hour at :00
    start_date=datetime(2026, 2, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["flowguard", "silver", "etl"],
) as silver_dag:

    def run_silver_etl(**context):
        """Call populate_silver.py for the logical date."""
        import subprocess
        date_str = context["ds"]  # YYYY-MM-DD of the DAG run
        result = subprocess.run(
            [VENV_PYTHON, f"{SCRIPTS_DIR}/populate_silver.py", "--date", date_str],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError(f"populate_silver.py failed for {date_str}")
        return f"Silver ETL complete for {date_str}"

    silver_task = PythonOperator(
        task_id="populate_silver",
        python_callable=run_silver_etl,
        provide_context=True,
    )


# ═══════════════════════════════════════════════════════════════════════════
# DAG 2: Gold ETL — runs daily at midnight
# Aggregates Silver → Gold for the previous day
# ═══════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id="flowguard_gold_etl",
    description="Silver → Gold aggregation (daily at midnight)",
    schedule_interval="0 0 * * *",   # daily at 00:00
    start_date=datetime(2026, 2, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["flowguard", "gold", "etl"],
) as gold_dag:

    def run_silver_for_gold(**context):
        """Ensure Silver is up-to-date before Gold aggregation."""
        import subprocess
        date_str = context["ds"]
        result = subprocess.run(
            [VENV_PYTHON, f"{SCRIPTS_DIR}/populate_silver.py", "--date", date_str],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError(f"populate_silver.py failed for {date_str}")

    def run_gold_etl(**context):
        """Call populate_gold.py for the logical date."""
        import subprocess
        date_str = context["ds"]
        result = subprocess.run(
            [VENV_PYTHON, f"{SCRIPTS_DIR}/populate_gold.py", "--date", date_str],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError(f"populate_gold.py failed for {date_str}")
        return f"Gold ETL complete for {date_str}"

    ensure_silver = PythonOperator(
        task_id="ensure_silver_up_to_date",
        python_callable=run_silver_for_gold,
        provide_context=True,
    )

    populate_gold = PythonOperator(
        task_id="populate_gold",
        python_callable=run_gold_etl,
        provide_context=True,
    )

    # Silver must complete before Gold
    ensure_silver >> populate_gold
