#!/bin/bash

# Health Check Script for FlowGuard Infrastructure
# Checks if all required services are running and healthy

set -e

echo "========================================="
echo "FlowGuard - Infrastructure Health Check"
echo "========================================="
echo ""

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check Docker
echo -n "Checking Docker... "
if command -v docker &> /dev/null; then
    echo -e "${GREEN}✓ Docker is installed${NC}"
else
    echo -e "${RED}✗ Docker is not installed${NC}"
    exit 1
fi

# Check Docker Compose
echo -n "Checking Docker Compose... "
if command -v docker-compose &> /dev/null || docker compose version &> /dev/null; then
    echo -e "${GREEN}✓ Docker Compose is available${NC}"
else
    echo -e "${RED}✗ Docker Compose is not installed${NC}"
    exit 1
fi

echo ""
echo "Checking FlowGuard Services:"
echo "========================================="

# Check Zookeeper
echo -n "Zookeeper... "
if docker ps | grep -q "flowguard-zookeeper"; then
    if docker exec flowguard-zookeeper nc -z localhost 2181 &> /dev/null; then
        echo -e "${GREEN}✓ Running and healthy${NC}"
    else
        echo -e "${YELLOW}⚠ Running but not responding${NC}"
    fi
else
    echo -e "${RED}✗ Not running${NC}"
fi

# Check Kafka Brokers
for i in 1 2 3; do
    echo -n "Kafka Broker $i... "
    if docker ps | grep -q "flowguard-kafka-$i"; then
        echo -e "${GREEN}✓ Running${NC}"
    else
        echo -e "${RED}✗ Not running${NC}"
    fi
done

# Check Kafka UI
echo -n "Kafka UI... "
if docker ps | grep -q "flowguard-kafka-ui"; then
    echo -e "${GREEN}✓ Running (http://localhost:8080)${NC}"
else
    echo -e "${YELLOW}⚠ Not running${NC}"
fi

echo ""
echo "Checking Kafka Topics:"
echo "========================================="

BOOTSTRAP_SERVERS="localhost:19092,localhost:19093,localhost:19094"

if docker ps | grep -q "flowguard-kafka-1"; then
    # List topics
    TOPICS=$(docker exec flowguard-kafka-1 kafka-topics --list --bootstrap-server kafka-broker-1:9092 2>/dev/null || echo "")
    
    if echo "$TOPICS" | grep -q "raw.orders.v1"; then
        echo -e "raw.orders.v1... ${GREEN}✓ Exists${NC}"
    else
        echo -e "raw.orders.v1... ${RED}✗ Missing${NC}"
    fi
    
    if echo "$TOPICS" | grep -q "raw.clicks.v1"; then
        echo -e "raw.clicks.v1... ${GREEN}✓ Exists${NC}"
    else
        echo -e "raw.clicks.v1... ${RED}✗ Missing${NC}"
    fi
else
    echo -e "${YELLOW}⚠ Cannot check topics - Kafka not running${NC}"
fi

echo ""
echo "========================================="
echo "Health Check Complete!"
echo "========================================="
