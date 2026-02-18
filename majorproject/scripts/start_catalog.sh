#!/bin/bash
# Start Food Catalog Service

cd "$(dirname "$0")/.."
export PYTHONPATH=$(pwd)

# Load .env so POSTGRES_PORT, POSTGRES_HOST etc. are picked up
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

echo "Starting Food Catalog Service..."
echo "PYTHONPATH: $PYTHONPATH"
echo "DB: ${POSTGRES_HOST:-localhost}:${POSTGRES_PORT:-5432}/${POSTGRES_DB:-food_catalog}"

# Kill any stale process on port 8001 to prevent "Address already in use"
echo "killing the existing port"
lsof -ti:8001 | xargs kill -9 2>/dev/null || true
sleep 1

/Users/anupdangi/Desktop/AnupAI/projects/2026/FlowGuard/venv/bin/python -m uvicorn \
    src.services.food_catalog.main:app \
    --host 0.0.0.0 \
    --port 8001 \
    --reload
