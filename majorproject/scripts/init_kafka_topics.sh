#!/bin/bash

# Kafka Topic Initialization Script
# Creates all topics defined in config/kafka/topics.yaml

set -e

echo "========================================="
echo "FlowGuard - Kafka Topic Initialization"
echo "========================================="

# Kafka connection details
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092,localhost:19093,localhost:19094}"

echo "Bootstrap Servers: $BOOTSTRAP_SERVERS"
echo ""

# Wait for Kafka to be ready
echo "Waiting for Kafka cluster to be ready..."
max_retries=30
retry_count=0

while [ $retry_count -lt $max_retries ]; do
    if kafka-topics --bootstrap-server $BOOTSTRAP_SERVERS --list > /dev/null 2>&1; then
        echo "✓ Kafka cluster is ready!"
        break
    fi
    retry_count=$((retry_count + 1))
    echo "Attempt $retry_count/$max_retries - Kafka not ready yet, waiting..."
    sleep 2
done

if [ $retry_count -eq $max_retries ]; then
    echo "✗ Failed to connect to Kafka after $max_retries attempts"
    exit 1
fi

echo ""
echo "Creating Kafka topics..."
echo "========================================="

# Topic 1: raw.orders.v1
echo "Creating topic: raw.orders.v1"
kafka-topics --create --if-not-exists \
    --bootstrap-server $BOOTSTRAP_SERVERS \
    --topic raw.orders.v1 \
    --partitions 3 \
    --replication-factor 2 \
    --config retention.ms=604800000 \
    --config compression.type=snappy \
    --config min.insync.replicas=2

echo "✓ Topic raw.orders.v1 created"
echo ""

# Topic 2: raw.clicks.v1
echo "Creating topic: raw.clicks.v1"
kafka-topics --create --if-not-exists \
    --bootstrap-server $BOOTSTRAP_SERVERS \
    --topic raw.clicks.v1 \
    --partitions 6 \
    --replication-factor 2 \
    --config retention.ms=172800000 \
    --config compression.type=snappy \
    --config min.insync.replicas=2

echo "✓ Topic raw.clicks.v1 created"
echo ""

# List all topics
echo "========================================="
echo "All Kafka Topics:"
echo "========================================="
kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVERS

echo ""
echo "========================================="
echo "Topic Details:"
echo "========================================="

# Describe each topic
kafka-topics --describe --bootstrap-server $BOOTSTRAP_SERVERS --topic raw.orders.v1
echo ""
kafka-topics --describe --bootstrap-server $BOOTSTRAP_SERVERS --topic raw.clicks.v1

echo ""
echo "========================================="
echo "✓ Kafka topics initialized successfully!"
echo "========================================="
