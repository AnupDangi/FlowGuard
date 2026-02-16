# FlowGuard MVP Progress Tracker

## ✅ Phase 1: Real-Time Event Streaming Infrastructure (COMPLETED)

### ✅ Stage 1: Events Gateway + Kafka Cluster (COMPLETED)

**Status:** Production-ready, pushed to GitHub

**Implemented:**

- ✅ FastAPI Events Gateway on port 8000
- ✅ Kafka cluster with 3 brokers (localhost:19092, 19093, 19094)
- ✅ Topics created: `raw.orders.v1` (3 partitions, RF=2), `raw.clicks.v1` (6 partitions, RF=2)
- ✅ Zookeeper on port 2181
- ✅ Kafka UI on port 8080 for visual monitoring
- ✅ Event schemas with Pydantic validation (OrderEvent, ClickEvent)
- ✅ Server-side UUID generation (production-ready, no collision risk)
- ✅ PostgreSQL integration for orders table (DB-first architecture)
- ✅ Orders stored in database before Kafka emission (source of truth)

**Architecture Flow:**

```
Client → Events Gateway → PostgreSQL (INSERT) → Kafka → Real-time Processing
```

---

### ✅ Stage 2: Reference Data Service + Web UI (COMPLETED)

**Status:** Production-ready, pushed to GitHub

**Implemented:**

- ✅ PostgreSQL database (port 5432) with food_catalog
- ✅ Food Catalog Service (FastAPI, port 8001) - reference data API
- ✅ 25 food items with detailed descriptions loaded from JSON
- ✅ Foods table with: food_id, name, category, price, description, image_url, is_available
- ✅ Orders table with: order_id (UUID), user_id, item_id, item_name, price, status, created_at
- ✅ Next.js 16 web app (port 3000) with TypeScript, Tailwind CSS 4
- ✅ Food catalog UI with category filtering
- ✅ Food detail page (/food/[foodId]) with full descriptions
- ✅ Order confirmation page (/order/[orderId]) with server-generated order_id
- ✅ Event tracking: impressions (hover), clicks (card), orders (button)
- ✅ Session-based user tracking with localStorage
- ✅ Client receives server-generated UUIDs (no client-side ID generation)

**API Endpoints:**

- GET /api/foods - List all food items with filtering
- GET /api/foods/{id} - Get single food item
- GET /api/foods/categories/list - List all categories
- POST /api/v1/orders/ - Submit order (returns server-generated order_id)
- POST /api/v1/clicks/ - Submit click/impression event

---

### ✅ Monitoring & Observability (COMPLETED)

**Status:** Production-ready

**Implemented:**

- ✅ Real-time event monitor (scripts/monitor_events.py) - colored Kafka consumer
- ✅ Service log aggregator (scripts/monitor_services.sh) - tail multiple logs
- ✅ Unified startup script (scripts/start_all.sh)
- ✅ Health check endpoints for all services
- ✅ Docker Compose orchestration with health checks
- ✅ Comprehensive monitoring guide (MONITORING_GUIDE.md)

---

## 🚧 Phase 2: Data Lake + Batch Processing (NEXT)

### Stage 3: Snowflake Bronze Layer (TODO)

**Goal:** Consume Kafka events and store in data warehouse

**To Implement:**

- [ ] Kafka consumer to read from raw.orders.v1 and raw.clicks.v1
- [ ] Snowflake BRONZE schema setup
- [ ] Batch writer to Snowflake (micro-batching, every 5 minutes)
- [ ] Raw event storage in Snowflake (immutable, append-only)
- [ ] Monitoring for consumer lag
- [ ] Dead letter queue for failed events

**Expected Output:**

```
Kafka (raw.orders.v1) → Consumer → Snowflake (BRONZE.orders_raw)
Kafka (raw.clicks.v1) → Consumer → Snowflake (BRONZE.clicks_raw)
```

---

### Stage 4: Flink Real-Time Processing (TODO)

**Goal:** Real-time attribution logic and stream enrichment

**To Implement:**

- [ ] Apache Flink job for click-to-order attribution
- [ ] Join clicks with orders in 30-minute window
- [ ] Calculate conversion metrics (CTR, conversion rate)
- [ ] Emit attributed events to new topic: `attributed.events.v1`
- [ ] Session management and user journey tracking
- [ ] Real-time alerting for anomalies

**Expected Output:**

```
Kafka (raw.clicks.v1 + raw.orders.v1) → Flink → attributed.events.v1
```

---

### Stage 5: Batch ETL with Spark + Airflow (TODO)

**Goal:** Scheduled data transformations and aggregations

**To Implement:**

- [ ] Apache Airflow for orchestration
- [ ] Spark jobs for data transformations
- [ ] SILVER layer: cleaned and validated data
- [ ] GOLD layer: aggregated metrics and KPIs
- [ ] Daily/hourly batch jobs
- [ ] Data quality checks

**Expected Tables:**

```
SILVER:
- orders_cleaned (validated, deduplicated)
- clicks_cleaned
- user_sessions (sessionized)

GOLD:
- daily_metrics (GMV, orders, users)
- food_item_performance
- user_cohorts
```

---

### Stage 6: Redis Billing + Analytics Dashboard (TODO)

**Goal:** Real-time billing and business intelligence

**To Implement:**

- [ ] Redis cache for real-time billing counters
- [ ] API for billing queries
- [ ] Metabase/Superset dashboard
- [ ] Key metrics visualization
- [ ] Alerts and notifications

---

## 📊 Current Status Summary

**Completed:** 2/6 stages (33%)

**Working Systems:**

- ✅ Kafka cluster (3 brokers, 2 topics, 9 partitions)
- ✅ Events Gateway with server-side UUID generation
- ✅ PostgreSQL with 2 tables (foods, orders)
- ✅ Food Catalog Service API
- ✅ Next.js web app with 3 pages
- ✅ Real-time event monitoring
- ✅ All code pushed to GitHub (9 commits)

**Next Milestone:** Snowflake Bronze Layer - consume Kafka events into data warehouse

---

## 🎯 Architecture Principles Followed

1. ✅ **Self-contained events** - Events include all business context
2. ✅ **Database-first** - Orders stored in DB before Kafka
3. ✅ **Server-side ID generation** - UUID v4 for uniqueness at scale
4. ✅ **Separation of concerns** - Reference data (PostgreSQL) vs Events (Kafka)
5. ✅ **Observability** - Monitoring, logging, health checks
6. ✅ **Production-ready** - No collision risk, proper error handling
