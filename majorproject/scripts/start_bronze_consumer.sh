#!/bin/bash
# Start Bronze Consumer
# Reads from Kafka and writes to Snowflake Bronze layer

set -e

cd "$(dirname "$0")/.."

echo "🚀 Starting Bronze Consumer..."
echo "================================"

# Check Snowflake password
if [ -z "$SNOWFLAKE_PASSWORD" ]; then
    echo "❌ Error: SNOWFLAKE_PASSWORD environment variable not set"
    echo "Usage: export SNOWFLAKE_PASSWORD='your_password'"
    exit 1
fi

# Activate virtual environment
source ../venv/bin/activate

# Run consumer
python -m src.consumers.bronze_consumer.main
