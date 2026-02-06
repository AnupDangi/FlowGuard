#!/bin/bash
# Start Food Catalog Service

cd "$(dirname "$0")/.."
export PYTHONPATH=$(pwd)

echo "Starting Food Catalog Service..."
echo "PYTHONPATH: $PYTHONPATH"

/Users/anupdangi/Desktop/AnupAI/projects/2026/FlowGuard/venv/bin/python -m uvicorn \
    src.services.food_catalog.main:app \
    --host 0.0.0.0 \
    --port 8001 \
    --reload
