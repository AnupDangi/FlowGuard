#!/bin/bash
# Start Bronze Consumer
# Reads from Kafka and writes to Snowflake Bronze layer

set -e

cd "$(dirname "$0")/.."
PROJECT_ROOT="$(pwd)"

# Load .env so SNOWFLAKE_PASSWORD and other vars are available
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

echo "🚀 Starting Bronze Consumer..."
echo "================================"

# Check Snowflake password
if [ -z "${SNOWFLAKE_PASSWORD:-}" ]; then
    echo "❌ Error: SNOWFLAKE_PASSWORD environment variable not set"
    echo "   Add SNOWFLAKE_PASSWORD to your .env file or export it manually."
    exit 1
fi

echo "✅ Snowflake: ${SNOWFLAKE_USER}@${SNOWFLAKE_ACCOUNT}"
echo "✅ Kafka:     ${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"

# Run consumer with explicit venv python and PYTHONPATH
PYTHONPATH="$PROJECT_ROOT" \
    "$PROJECT_ROOT/../venv/bin/python" -m src.consumers.bronze_consumer.main
