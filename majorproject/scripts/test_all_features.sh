#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_PYTHON="/Users/anupdangi/Desktop/AnupAI/projects/2026/FlowGuard/venv/bin/python"

cd "$PROJECT_ROOT"

echo "== FlowGuard full feature test =="
echo "[1/4] Phase 1 + 2 HTTP verification"
"$VENV_PYTHON" scripts/verify_phases.py

echo "[2/4] Phase 3 analytics verification"
"$VENV_PYTHON" scripts/verify_phase3.py

echo "[3/4] Pipeline monitor snapshot"
"$VENV_PYTHON" scripts/monitor_pipeline.py

echo "[4/4] Done"
echo "All available automated feature checks completed."
