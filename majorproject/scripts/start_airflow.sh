#!/usr/bin/env bash
# =============================================================================
# Start Airflow for FlowGuard
#
# Usage:
#   ./scripts/start_airflow.sh          # start Airflow
#   ./scripts/start_airflow.sh stop     # stop Airflow
#   ./scripts/start_airflow.sh logs     # tail scheduler logs
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/airflow/docker-compose.airflow.yml"

# Load .env so Snowflake creds are available to Airflow containers
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

ACTION="${1:-start}"

case "$ACTION" in
    start)
        echo "🚀 Starting FlowGuard Airflow..."
        echo "   Compose file: $COMPOSE_FILE"

        # Ensure the flowguard-network exists (created by docker-compose.yml)
        if ! docker network inspect flowguard-network &>/dev/null; then
            echo "⚠️  flowguard-network not found. Start main services first:"
            echo "   docker compose up -d"
            exit 1
        fi

        # Create Airflow directories
        mkdir -p "$PROJECT_ROOT/airflow/logs"
        mkdir -p "$PROJECT_ROOT/airflow/plugins"

        # Start Airflow
        docker compose -f "$COMPOSE_FILE" up -d --remove-orphans

        echo ""
        echo "⏳ Waiting for Airflow webserver to be ready..."
        for i in $(seq 1 30); do
            if curl -sf http://localhost:8080/health &>/dev/null; then
                echo "✅ Airflow is ready!"
                break
            fi
            sleep 5
            echo "   Still waiting... ($((i*5))s)"
        done

        echo ""
        echo "════════════════════════════════════════"
        echo "  Airflow UI:  http://localhost:8080"
        echo "  Username:    admin"
        echo "  Password:    admin"
        echo ""
        echo "  DAGs:"
        echo "    flowguard_silver_etl  (hourly)"
        echo "    flowguard_gold_etl    (daily)"
        echo "════════════════════════════════════════"
        ;;

    stop)
        echo "🛑 Stopping Airflow..."
        docker compose -f "$COMPOSE_FILE" down
        echo "✅ Airflow stopped"
        ;;

    logs)
        docker compose -f "$COMPOSE_FILE" logs -f airflow-scheduler
        ;;

    restart)
        "$0" stop
        sleep 3
        "$0" start
        ;;

    *)
        echo "Usage: $0 [start|stop|logs|restart]"
        exit 1
        ;;
esac
