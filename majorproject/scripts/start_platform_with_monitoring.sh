#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

echo "========================================="
echo "Starting FlowGuard Platform + Monitoring"
echo "========================================="

echo "1) Starting core platform..."
./scripts/start_all.sh

echo "2) Starting monitoring stack..."
docker compose -f infrastructure/monitoring/docker-compose.yml up -d

echo "3) Quick health checks..."
echo "- Prometheus: $(curl -sS http://localhost:9090/-/ready || echo unavailable)"
echo "- Grafana: $(curl -sS http://localhost:3001/api/health || echo unavailable)"

echo "========================================="
echo "Ready"
echo "========================================="
echo "Core:"
echo "  - Gateway: http://localhost:8000/health"
echo "  - Catalog: http://localhost:8001/health"
echo "Monitoring:"
echo "  - Prometheus: http://localhost:9090"
echo "  - Grafana: http://localhost:3001 (admin/admin)"
