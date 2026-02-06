#!/bin/bash
# Monitor all FlowGuard services in one terminal

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}FlowGuard Service Monitor${NC}"
echo -e "${GREEN}========================================${NC}"

# Check if services are running
echo -e "\n${BLUE}Checking service status...${NC}"

# Food Catalog
if lsof -Pi :8001 -sTCP:LISTEN -t >/dev/null ; then
    echo -e "${GREEN}✅ Food Catalog Service (port 8001)${NC}"
else
    echo -e "${RED}❌ Food Catalog Service NOT running${NC}"
fi

# Events Gateway
if lsof -Pi :8000 -sTCP:LISTEN -t >/dev/null ; then
    echo -e "${GREEN}✅ Events Gateway Service (port 8000)${NC}"
else
    echo -e "${RED}❌ Events Gateway Service NOT running${NC}"
fi

# Kafka
if docker ps | grep -q "flowguard-kafka-1"; then
    echo -e "${GREEN}✅ Kafka Cluster (3 brokers)${NC}"
else
    echo -e "${RED}❌ Kafka Cluster NOT running${NC}"
fi

# PostgreSQL
if docker ps | grep -q "flowguard-postgres"; then
    echo -e "${GREEN}✅ PostgreSQL Database${NC}"
else
    echo -e "${RED}❌ PostgreSQL NOT running${NC}"
fi

echo -e "\n${YELLOW}========================================${NC}"
echo -e "${YELLOW}Tailing Service Logs (Ctrl+C to stop)${NC}"
echo -e "${YELLOW}========================================${NC}\n"

# Use multitail or tail multiple files
if command -v multitail &> /dev/null; then
    multitail \
        -l "tail -f /tmp/catalog.log" \
        -l "tail -f /tmp/events_gateway.log"
else
    # Fallback to tail with labels
    tail -f /tmp/catalog.log /tmp/events_gateway.log
fi
