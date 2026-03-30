# FlowGuard - End-to-End Zomato-Style Data Platform

FlowGuard is a local-first data engineering platform for event ingestion, real-time personalization/fraud detection, and batch analytics pipelines.

It includes:
- event APIs (FastAPI),
- Kafka ingestion,
- Bronze/Silver/Gold analytics in PostgreSQL,
- Airflow orchestration,
- real-time consumers for personalization and fraud,
- and full observability with Prometheus + Grafana.

## Implemented Architecture

![FlowGuard Architecture](./ZomatoDataEngineering.png)

## What This Repo Contains

- `src/services/events_gateway/`: Auth + event ingestion APIs (orders, clicks, behavior, fraud, ads)
- `src/consumers/bronze_consumer/`: Kafka -> Bronze writes
- `src/pipelines/`: Batch and real-time pipeline code
- `scripts/`: Setup, startup, verification, and monitoring scripts
- `infrastructure/monitoring/`: Prometheus, Grafana, exporters, alerting
- `zomato-web-app/`: Next.js frontend with auth and event tracking

## Prerequisites

- Docker + Docker Compose
- Python 3.10+
- Node.js + pnpm (for frontend)

## Environment Setup

1. Create env file:

```bash
cp .env.example .env
```

2. Install Python deps:

```bash
/Users/anupdangi/Desktop/AnupAI/projects/2026/FlowGuard/venv/bin/pip install -r requirements.txt
```

3. Install frontend deps:

```bash
cd zomato-web-app && pnpm install && cd ..
```

## Start the Platform

### Option A: Core platform only

```bash
./scripts/start_all.sh
```

### Option B: Core platform + monitoring (recommended)

```bash
./scripts/start_platform_with_monitoring.sh
```

### Start additional pipeline workers

```bash
./scripts/start_bronze_consumer.sh
./scripts/start_airflow.sh
./scripts/start_flink.sh personalize-python
./scripts/start_flink.sh fraud-python
```

### Start frontend

```bash
cd zomato-web-app && pnpm dev
```

## Service Endpoints

- Events Gateway: `http://localhost:8000`
- Food Catalog API: `http://localhost:8001`
- Kafka UI: `http://localhost:8081`
- Airflow: `http://localhost:8080`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3001` (`admin` / `admin`)

## Data Flow (Implemented)

1. Frontend sends auth-protected order/click/behavior events to Events Gateway.
2. Gateway enforces JWT identity and publishes canonical events to Kafka.
3. Bronze consumer ingests Kafka events into `analytics.bronze_*` tables.
4. Airflow DAGs run Silver/Gold transforms into `analytics.silver_*` and `analytics.gold_*`.
5. Real-time jobs update Redis signals for personalization and fraud alerts.
6. Recon process updates Redis totals safely (overwrite-if-greater strategy).

## Monitoring and Alerting

Monitoring stack lives in `infrastructure/monitoring/` and includes:
- Prometheus scraping gateway, Kafka, Redis, Postgres, blackbox health, custom pipeline exporter.
- Grafana auto-provisioned datasource and `FlowGuard Overview` dashboard.
- Alert rules for gateway down, Kafka/Postgres exporter down, Airflow health fail, ETL stale, fraud spikes.

Detailed guide: `docs/monitoring/README.md`

## Verification and Testing

Run full automated checks:

```bash
./scripts/test_all_features.sh
```

This validates:
- Phase 1+2 APIs (`scripts/verify_phases.py`)
- Phase 3 analytics table/query readiness (`scripts/verify_phase3.py`)
- Pipeline monitor snapshot (`scripts/monitor_pipeline.py`)

Also validate frontend build:

```bash
cd zomato-web-app && npm run build
```

## Useful Commands

```bash
# Check DAG and analytics readiness
python scripts/check_dag_readiness.py
python scripts/verify_pipeline.py

# Tail service logs
./scripts/monitor_services.sh

# Restart monitoring stack only
docker compose -f infrastructure/monitoring/docker-compose.yml up -d
```

## Notes

- Default runtime backend is PostgreSQL analytics (Snowflake is optional/legacy).
- If you need a clean analytics reset, remove `analytics-postgres-data` volume and restart Docker services.