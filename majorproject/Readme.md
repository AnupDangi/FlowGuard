# FlowGuard - Real-Time Event Processing Pipeline

Production-grade event streaming system for food delivery analytics (Zomato-inspired).

## Architecture (Current: Stage 2)

```
PostgreSQL (port 5432)     Kafka Cluster (19092-19094)
     ↓                              ↑
Food Catalog API (8001)    Events Gateway API (8000)
     ↓                              ↑
     └─────→ Web UI (3000) ────────┘
```

**Separation of concerns:**

- Reference data (food items) → PostgreSQL + Food Catalog Service
- Event data (orders/clicks) → Events Gateway + Kafka

---

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
cd zomato-web-app && pnpm install && cd ..
```

### 2. Start Everything (One Command)

```bash
./scripts/start_all.sh
```

### 3. Start Frontend

```bash
cd zomato-web-app && pnpm dev
```

### 4. Monitor Events (Real-Time)

```bash
# Terminal 1: Event monitor
python scripts/monitor_events.py

# Terminal 2: Service logs
./scripts/monitor_services.sh

# Terminal 3: Test events
python scripts/test_send_event.py
```

### 5. Open Application

- Web UI: http://localhost:3000
- Food Catalog API: http://localhost:8001/docs
- Events Gateway API: http://localhost:8000/docs
- Kafka UI: http://localhost:8080

---

## Scripts

`./scripts/start_all.sh` - Start all services (bash script)  
`./scripts/start_catalog.sh` - Start Food Catalog only (bash script)  
`python scripts/monitor_events.py` - Real-time Kafka event monitor  
`./scripts/monitor_services.sh` - Tail all service logs (bash script)  
`python scripts/test_send_event.py` - Send test order event  
`python scripts/test_catalog_api.py` - Test Food Catalog API  
`python src/main.py start-gateway` - Start Events Gateway (CLI)  
`python src/main.py start-catalog` - Start Food Catalog (CLI)  
`python src/main.py health-check` - Check Kafka status

---

## What Works (Stage 2 Complete)

✅ PostgreSQL - 25 food items  
✅ Food Catalog Service - REST API (GET)  
✅ Events Gateway Service - REST API (POST)  
✅ Web UI - Browse, filter, order  
✅ Event Tracking - Orders → Kafka  
✅ Kafka Cluster - 3 brokers, 2 topics

---

## Data Flow

**Reference Data:** food_items.json → PostgreSQL → Food Catalog API → UI  
**Event Data:** UI → Events Gateway → Kafka → Snowflake (future)

---

## Services

**Food Catalog (8001):** GET /api/foods, GET /api/foods/{id}, GET /health  
**Events Gateway (8000):** POST /api/v1/orders, POST /api/v1/clicks, GET /health

---

## Tech Stack

FastAPI, PostgreSQL, Kafka, Next.js 16, React 19, TypeScript, Tailwind, Docker

---

## Future (Stage 3+)

Kafka → Snowflake Bronze → Flink attribution → Spark ETL → Redis billing

---

## Original Architecture (Future)

```
User Action (Web App)
↓
[Events Gateway FastAPI]
↓
[Kafka Cluster 1: raw.orders.v1]
↓ (2 consumers)
├──→ [Data Lake] → S3 → [Airflow ETL] → [Kafka 3: recon.v1]
└──→ [Flink] → [Kafka 2: attributed.events.v1] → [Notifications] → [Redis + User]
```
