#!/usr/bin/env bash
# =============================================================================
# Start Flink real-time attribution for FlowGuard
#
# Usage:
#   ./scripts/start_flink.sh              # start Flink cluster + submit job
#   ./scripts/start_flink.sh python       # run Python fallback (no Flink cluster)
#   ./scripts/start_flink.sh personalize-python  # run personalization updater
#   ./scripts/start_flink.sh fraud-python       # run fraud detection consumer
#   ./scripts/start_flink.sh stop         # stop Flink cluster
#   ./scripts/start_flink.sh logs         # tail job logs
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_PYTHON="/Users/anupdangi/Desktop/AnupAI/projects/2026/FlowGuard/venv/bin/python"

# Load .env
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

ACTION="${1:-start}"

case "$ACTION" in
    start)
        echo "🚀 Starting FlowGuard Flink Attribution..."

        # Ensure flowguard-network and Redis exist
        if ! docker network inspect flowguard-network &>/dev/null; then
            echo "❌ flowguard-network not found. Run: docker compose up -d"
            exit 1
        fi
        if ! docker ps --format '{{.Names}}' | grep -q flowguard-redis; then
            echo "❌ Redis not running. Run: docker compose up -d redis"
            exit 1
        fi

        docker compose -f "$PROJECT_ROOT/infrastructure/flink/docker-compose.yml" up -d

        echo ""
        echo "⏳ Waiting for Flink JobManager..."
        for i in $(seq 1 20); do
            if curl -sf http://localhost:8082/overview &>/dev/null; then
                echo "✅ Flink is ready!"
                break
            fi
            sleep 5
            echo "   Still waiting... ($((i*5))s)"
        done

        echo ""
        echo "════════════════════════════════════════"
        echo "  Flink UI:  http://localhost:8082"
        echo "  Job:       FlowGuard Real-Time Attribution"
        echo ""
        echo "  Redis counters (after events):"
        echo "    redis-cli -p 6379 KEYS 'ads:*'"
        echo "════════════════════════════════════════"
        ;;

    python)
        echo "🐍 Starting Python fallback attribution consumer..."
        echo "   (No Flink cluster needed — uses confluent-kafka directly)"
        echo ""

        # Ensure Redis is reachable
        if ! redis-cli -p 6379 ping &>/dev/null 2>&1; then
            echo "⚠️  Redis not reachable on localhost:6379"
            echo "   Start it with: docker compose up -d redis"
            exit 1
        fi

        export ATTRIBUTION_MODE=python
        export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092,localhost:19093,localhost:19094}"
        export REDIS_HOST=localhost
        export REDIS_PORT=6379

        exec "$VENV_PYTHON" "$PROJECT_ROOT/src/pipelines/realtime/flink/realtime_attribution.py" --python
        ;;

    personalize-python)
        echo "🧠 Starting personalization profile updater..."
        export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092,localhost:19093,localhost:19094}"
        export REDIS_HOST=localhost
        export REDIS_PORT=6379
        exec "$VENV_PYTHON" "$PROJECT_ROOT/src/pipelines/realtime/flink/personalization_profile_updater.py"
        ;;

    fraud-python)
        echo "🛡️ Starting fraud detection job (behavior → fraud.alerts.v1)..."
        export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092,localhost:19093,localhost:19094}"
        export REDIS_HOST=localhost
        export REDIS_PORT=6379
        exec "$VENV_PYTHON" "$PROJECT_ROOT/src/pipelines/realtime/flink/fraud_detection.py"
        ;;

    stop)
        echo "🛑 Stopping Flink cluster..."
        docker compose -f "$PROJECT_ROOT/infrastructure/flink/docker-compose.yml" down
        echo "✅ Flink stopped"
        ;;

    logs)
        docker compose -f "$PROJECT_ROOT/infrastructure/flink/docker-compose.yml" logs -f flink-job-submitter
        ;;

    status)
        echo "Flink jobs:"
        curl -s http://localhost:8082/jobs | python3 -m json.tool 2>/dev/null || echo "Flink not running"
        echo ""
        echo "Redis ad counters (today):"
        TODAY=$(date +%Y-%m-%d)
        redis-cli -p 6379 KEYS "ads:*:${TODAY}" 2>/dev/null | sort || echo "Redis not running"
        ;;

    *)
        echo "Usage: $0 [start|python|personalize-python|fraud-python|stop|logs|status]"
        exit 1
        ;;
esac
