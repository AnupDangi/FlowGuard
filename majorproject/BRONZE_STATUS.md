# FlowGuard Bronze Layer - Current Status & Gaps

## ✅ WHAT'S WORKING (Real-time event streaming)

### Infrastructure

- ✅ Kafka 3-broker cluster (healthy, proper partitions)
- ✅ PostgreSQL with orders + foods tables
- ✅ Snowflake Bronze layer (ORDERS_RAW, CLICKS_RAW)
- ✅ Events Gateway API (FastAPI, port 8000)
- ✅ Food Catalog API (FastAPI, port 8001)

### Data Pipeline

- ✅ **Kafka → Snowflake ingestion WORKING**
  - Bronze consumer running (PID check: see logs)
  - Batch size: 100 events OR 5-second timeout
  - INSERT...SELECT PARSE_JSON(%s) syntax (only method that works!)
  - Manual offset commit AFTER Snowflake write (no data loss)

### Current Ingestion Stats

- 23 orders in Snowflake
- 70 clicks in Snowflake
- Last minute: 6 new orders
- Flushing every ~5 seconds as events arrive

### Test Infrastructure

- ✅ Traffic generator script (`scripts/generate_test_traffic.py`)
- ✅ Snowflake monitor script (`scripts/monitor_snowflake_ingestion.py`)
- ✅ Quick check script (`scripts/quick_check.py`)

---

## ❌ WHAT'S MISSING (Production-grade features)

### 1. **Production Metadata Columns** (CRITICAL)

**Current schema:**

```sql
CREATE TABLE ORDERS_RAW (
    EVENT_ID VARCHAR(100),
    ORDER_ID VARCHAR(100),
    USER_ID INTEGER,
    ...
    RAW_EVENT VARIANT  -- ✅ This is correct
)
```

**Missing columns:**

```sql
EVENT_TYPE VARCHAR(50)        -- ❌ Can't filter by event type without parsing RAW_EVENT
SCHEMA_VERSION VARCHAR(20)     -- ❌ Can't handle v1 → v2 schema evolution
SOURCE_TOPIC VARCHAR(100)      -- ❌ Can't debug which Kafka topic produced event
PRODUCER_SERVICE VARCHAR(100)  -- ❌ Can't trace which service emitted event
INGESTION_ID VARCHAR(200)      -- ❌ No idempotency on consumer restarts
KAFKA_PARTITION INTEGER        -- ❌ Can't track which partition
KAFKA_OFFSET BIGINT            -- ❌ Can't replay specific offset range
```

**Why this matters:**

- **3 AM debugging:** When production breaks, you need SOURCE_TOPIC + KAFKA_OFFSET to replay
- **Schema evolution:** When v2 events arrive, SCHEMA_VERSION lets Silver handle both
- **Idempotency:** Consumer restarts = duplicate events without INGESTION_ID
- **Ownership:** PRODUCER_SERVICE tells you which team to page

---

### 2. **Idempotency Guarantee** (HIGH PRIORITY)

**Current problem:**

```python
# Consumer crashes at 10:05 PM
# Kafka offset = 1000 (committed)
# Consumer restarts at 10:06 PM
# Re-consumes messages 1000-1050
# Result: DUPLICATE EVENTS in Snowflake 😱
```

**Production solution:**

```python
ingestion_id = f"{topic}-{partition}-{offset}"

# Use MERGE instead of INSERT
MERGE INTO ORDERS_RAW USING (VALUES ...) ON INGESTION_ID = ...
WHEN NOT MATCHED THEN INSERT ...
```

**Why this matters:**

- Financial calculations depend on accurate event counts
- Fraud detection = false positives with duplicates
- Compliance audits fail if event counts don't match

---

### 3. **Dead Letter Queue (DLQ)** (MEDIUM PRIORITY)

**Current problem:**

```python
# Malformed event from Kafka
# Consumer crashes
# Pipeline stops
# All downstream systems starve
```

**Production solution:**

```sql
CREATE TABLE ORDERS_DLQ (
    FAILED_AT TIMESTAMP,
    ERROR_MESSAGE STRING,
    RAW_PAYLOAD VARIANT,
    KAFKA_METADATA VARIANT
)
```

```python
try:
    snowflake_writer.write_batch(events)
except Exception as e:
    dlq_writer.write_to_dlq(events, error=str(e))
    # Pipeline continues!
```

**Why this matters:**

- One bad event shouldn't kill the entire pipeline
- DLQ lets you investigate failures without losing data
- Production systems NEVER crash on bad data

---

### 4. **Consumer Lag Monitoring** (MEDIUM PRIORITY)

**Current blind spot:**

```
❓ How far behind is the consumer?
❓ Is Snowflake slower than Kafka ingestion rate?
❓ Will we hit batch_timeout more than batch_size?
```

**Production solution:**

```python
# Expose metrics:
metrics = {
    'kafka_lag': current_offset - committed_offset,
    'events_per_second': events_processed / elapsed_time,
    'avg_batch_size': sum(batch_sizes) / num_batches,
    'snowflake_write_latency_p99': percentile(write_times, 99)
}
```

**Why this matters:**

- Lag > 10K messages = system can't keep up
- Need to add more consumer instances (parallel partitions)
- Helps predict when to scale infrastructure

---

### 5. **Schema Version Tracking** (LOW PRIORITY, but future-proof)

**Current problem:**

```json
// v1 event (today)
{"order_id": "123", "user_id": 42, "price": 10.99}

// v2 event (next month - field renamed)
{"order_id": "123", "customer_id": 42, "price": 10.99}
```

Bronze stores both (✅), but Silver can't distinguish them (❌)

**Production solution:**

```python
# Producer adds version
event['schema_version'] = 'v1'

# Silver handles both:
COALESCE(
    RAW_EVENT:user_id::INT,      -- v1
    RAW_EVENT:customer_id::INT    -- v2
) AS user_id
```

---

## 📊 WHAT YOU'RE MISSING RIGHT NOW

### Monitoring Gap

You asked: **"I want to see logs going to Snowflake in a particular time period"**

**What exists:**

- ✅ `scripts/monitor_snowflake_ingestion.py` - real-time dashboard
- ✅ `scripts/quick_check.py` - one-shot status check
- ✅ Consumer logs in `/tmp/bronze_consumer.log`

**How to run:**

```bash
# Terminal 1: Monitor Snowflake (refreshes every 3s)
cd majorproject
../venv/bin/python scripts/monitor_snowflake_ingestion.py

# Terminal 2: Watch consumer logs
tail -f /tmp/bronze_consumer.log

# Terminal 3: Check Kafka lag
docker exec flowguard-kafka-1 kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --group bronze-consumer-group \
    --describe
```

### Production-Grade Upgrade Path

**Priority 1 (Do first):**

1. Add metadata columns (EVENT_TYPE, SOURCE_TOPIC, INGESTION_ID, SCHEMA_VERSION)
2. Implement idempotency with MERGE
3. Update consumer to populate new fields

**Priority 2 (Do before production):** 4. Add DLQ tables + error handling 5. Implement consumer lag metrics 6. Add monitoring alerts

**Priority 3 (Nice to have):** 7. Parallel consumer instances per partition 8. Snowflake clustering keys for performance 9. Backfill jobs for historical data

---

## 🎯 RECOMMENDED NEXT STEP

**Option A: Add Production Metadata NOW** (30 min)

- Prevents future schema migrations (painful)
- Enables debugging from day 1
- Required for fraud detection (Silver layer)

**Option B: Test Web UI First** (15 min)

- Verify end-to-end user flow works
- Then upgrade schema once baseline proven

**My recommendation:** **Option A**

- Bronze schema changes are expensive later
- Adding columns now = zero downtime
- Backfilling metadata = impossible without Kafka replay

---

## 🚨 CURRENT SYSTEM VULNERABILITIES

1. **Consumer restart = duplicate events** (no INGESTION_ID)
2. **Can't debug production issues** (no SOURCE_TOPIC, KAFKA_OFFSET)
3. **Can't handle schema evolution** (no SCHEMA_VERSION)
4. **Bad event = pipeline crash** (no DLQ)
5. **Blind to performance** (no lag monitoring)

---

**You're 70% to production-grade.**
**The remaining 30% = difference between "it works" and "it survives real users."**
