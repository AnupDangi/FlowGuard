#!/bin/bash
# Start FlowGuard backend services with proper environment

cd "$(dirname "$0")/.."

echo "🚀 Starting FlowGuard Backend Services..."

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
    echo "✅ Loaded .env file"
else
    echo "⚠️  Warning: .env file not found, using defaults"
fi

# Activate virtual environment
source ../venv/bin/activate

# Kill existing processes
pkill -9 -f food_catalog 2>/dev/null
pkill -9 -f events_gateway 2>/dev/null
sleep 1

echo ""
echo "1️⃣  Starting Food Catalog Service (port 8001)..."
python -m uvicorn src.services.food_catalog.main:app \
    --host 0.0.0.0 \
    --port 8001 \
    --reload \
    > /tmp/food_catalog.log 2>&1 &

sleep 2

echo "2️⃣  Starting Events Gateway Service (port 8000)..."
python -m uvicorn src.services.events_gateway.main:app \
    --host 0.0.0.0 \
    --port 8000 \
    --reload \
    > /tmp/events_gateway.log 2>&1 &

sleep 3

echo ""
echo "=== Verifying Services ==="

# Check Food Catalog
if curl -s --max-time 2 http://localhost:8001/health > /dev/null 2>&1; then
    echo "✅ Food Catalog: http://localhost:8001"
else
    echo "❌ Food Catalog: Failed to start (check /tmp/food_catalog.log)"
fi

# Check Events Gateway
if curl -s --max-time 2 http://localhost:8000/health > /dev/null 2>&1; then
    echo "✅ Events Gateway: http://localhost:8000"
else
    echo "❌ Events Gateway: Failed to start (check /tmp/events_gateway.log)"
fi

echo ""
echo "📝 Logs: tail -f /tmp/food_catalog.log /tmp/events_gateway.log"
