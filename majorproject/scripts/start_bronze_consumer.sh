#!/bin/bash
# Start Bronze Consumer
# Reads from Kafka and writes to local PostgreSQL analytics Bronze layer

set -e

cd "$(dirname "$0")/.."
PROJECT_ROOT="$(pwd)"

# Load .env so analytics DB and Kafka vars are available
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

echo "🚀 Starting Bronze Consumer..."
echo "================================"

# Check analytics DB password
if [ -z "${ANALYTICS_POSTGRES_PASSWORD:-}" ]; then
    echo "❌ Error: ANALYTICS_POSTGRES_PASSWORD environment variable not set"
    echo "   Add ANALYTICS_POSTGRES_PASSWORD to your .env file or export it manually."
    exit 1
fi

echo "✅ Analytics DB: ${ANALYTICS_POSTGRES_USER:-flowguard}@${ANALYTICS_POSTGRES_HOST:-localhost}:${ANALYTICS_POSTGRES_PORT:-5434}/${ANALYTICS_POSTGRES_DB:-flowguard_analytics}"
echo "✅ Kafka:     ${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"

# Run consumer with explicit venv python and PYTHONPATH
PYTHONPATH="$PROJECT_ROOT" \
    "$PROJECT_ROOT/../venv/bin/python" -m src.consumers.bronze_consumer.main
