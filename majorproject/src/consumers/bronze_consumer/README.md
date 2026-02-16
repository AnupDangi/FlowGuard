# Bronze Consumer - Kafka → Snowflake

Schema-flexible consumer that reads events from Kafka and batch-writes to Snowflake Bronze layer.

## Architecture

```
Kafka Topics                 Bronze Consumer              Snowflake Bronze
─────────────────           ──────────────────           ────────────────
raw.orders.v1    ─┐         ┌─> Batch (100)  ─┐          ┌─> ORDERS_RAW
                  ├────────>│                 ├────────>│   (VARIANT)
raw.clicks.v1    ─┘         └─> Batch (5s)   ─┘          └─> CLICKS_RAW
                                                              (VARIANT)
```

## Features

- **Micro-batching**: 100 events OR 5 seconds (whichever comes first)
- **Schema Evolution**: VARIANT column stores full JSON, handles any future fields
- **Manual Offset Commit**: Only commits after successful Snowflake write
- **Graceful Shutdown**: Flushes remaining batches on SIGTERM/SIGINT
- **Error Handling**: Logs errors, preserves data integrity

## Schema Flexibility

**Today's Event:**

```json
{
  "order_id": "uuid",
  "user_id": 123,
  "item_id": 5,
  "price": 12.99
}
```

**Tomorrow's Event (new fields added):**

```json
{
  "order_id": "uuid",
  "user_id": 123,
  "item_id": 5,
  "price": 12.99,
  "delivery_address": "123 Main St", // New field!
  "payment_method": "card" // New field!
}
```

**✅ Consumer works with both - no code changes needed!**

Query new fields from VARIANT:

```sql
SELECT
    ORDER_ID,
    RAW_EVENT:delivery_address::STRING as address
FROM BRONZE.ORDERS_RAW;
```

## Setup

### 1. Install Dependencies

```bash
pip install confluent-kafka snowflake-connector-python
```

### 2. Set Environment Variables

```bash
export SNOWFLAKE_PASSWORD='your_password'
```

Optional overrides:

```bash
export BATCH_SIZE=100
export BATCH_TIMEOUT=5.0
export CONSUMER_GROUP_ID=bronze-consumer-group
export LOG_LEVEL=INFO
```

### 3. Initialize Snowflake Schema

```bash
./scripts/init_snowflake.sh
```

Creates tables:

- `BRONZE.ORDERS_RAW` - Order events with VARIANT column
- `BRONZE.CLICKS_RAW` - Click/impression events with VARIANT column

### 4. Start Consumer

```bash
./scripts/start_bronze_consumer.sh
```

Logs to:

- stdout (console)
- `/tmp/bronze_consumer.log`

## Monitoring

**Consumer Logs:**

```
2026-02-07 10:30:00 - INFO - Consumer initialized: group=bronze-consumer-group
2026-02-07 10:30:05 - INFO - Flushing 100 orders to Snowflake...
2026-02-07 10:30:06 - INFO - Inserted 100 orders into Snowflake
2026-02-07 10:30:06 - INFO - Kafka offsets committed
```

**Query Snowflake:**

```sql
-- Check record counts
SELECT 'ORDERS' as TABLE_NAME, COUNT(*) FROM BRONZE.ORDERS_RAW
UNION ALL
SELECT 'CLICKS', COUNT(*) FROM BRONZE.CLICKS_RAW;

-- Check recent events
SELECT * FROM BRONZE.ORDERS_RAW
ORDER BY INGESTION_TIMESTAMP DESC
LIMIT 10;

-- Query VARIANT column for new fields
SELECT
    ORDER_ID,
    RAW_EVENT:delivery_address::STRING,
    RAW_EVENT:payment_method::STRING
FROM BRONZE.ORDERS_RAW
WHERE RAW_EVENT:delivery_address IS NOT NULL;
```

**Consumer Lag:**

```bash
kafka-consumer-groups --bootstrap-server localhost:19092 \
  --group bronze-consumer-group \
  --describe
```

## How It Works

### Batching Logic

1. **Consume** from Kafka (1 second poll timeout)
2. **Add to batch** (orders or clicks)
3. **Flush when:**
   - Batch reaches 100 events, OR
   - 5 seconds elapsed since last flush
4. **Write to Snowflake** (bulk INSERT)
5. **Commit offsets** (only after successful write)

### Error Handling

- **Snowflake write fails**: Logs error, keeps offsets uncommitted, retries on next batch
- **JSON parse error**: Logs error, skips message, continues
- **Connection lost**: Reconnects automatically on next operation
- **Shutdown signal**: Flushes remaining batches before exit

### Offset Management

```python
# ✅ Correct: Commit only after Snowflake success
writer.write_orders_batch(batch)
consumer.commit()  # Safe!

# ❌ Wrong: Commit before write
consumer.commit()  # Data loss risk!
writer.write_orders_batch(batch)
```

## Testing

### Generate Test Events

Place orders from UI:

```bash
open http://localhost:3000
# Click food items and place orders
```

Or use curl:

```bash
curl -X POST http://localhost:8000/api/v1/orders/ \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 123,
    "item_id": 5,
    "item_name": "Chicken Biryani",
    "price": 12.99
  }'
```

### Verify Snowflake

```sql
-- Should see events appearing
SELECT COUNT(*) FROM BRONZE.ORDERS_RAW;

-- Check RAW_EVENT VARIANT
SELECT RAW_EVENT FROM BRONZE.ORDERS_RAW LIMIT 1;

-- Query specific fields
SELECT
    ORDER_ID,
    USER_ID,
    RAW_EVENT:item_name::STRING as item,
    RAW_EVENT:price::NUMBER as price
FROM BRONZE.ORDERS_RAW;
```

## Configuration

**config.py** settings:

```python
BATCH_SIZE = 100              # Events per batch
BATCH_TIMEOUT = 5.0           # Seconds to wait
CONSUMER_GROUP = 'bronze-consumer-group'
AUTO_OFFSET_RESET = 'earliest'  # Start from beginning
ENABLE_AUTO_COMMIT = False    # Manual commit only
```

## Troubleshooting

**Consumer not connecting to Kafka:**

```bash
# Check Kafka is running
docker ps | grep kafka

# Test connectivity
kafka-topics --bootstrap-server localhost:19092 --list
```

**Snowflake connection failed:**

```bash
# Check password is set
echo $SNOWFLAKE_PASSWORD

# Test connection
python3 -c "
import snowflake.connector
conn = snowflake.connector.connect(
    account='ZLNJTCF-KE38237',
    user='ANUPDANGI12',
    password='$SNOWFLAKE_PASSWORD'
)
print('Connected!')
"
```

**No events in Snowflake:**

```bash
# Check consumer logs
tail -f /tmp/bronze_consumer.log

# Check Kafka has events
./scripts/monitor_events.py
```

## Production Considerations

### Scaling

- **Multiple consumers**: Set different `CONSUMER_GROUP_ID` or increase partition count
- **Larger batches**: Increase `BATCH_SIZE` for higher throughput
- **Faster flushes**: Decrease `BATCH_TIMEOUT` for lower latency

### Monitoring

Add to production:

- Prometheus metrics (consumer lag, batch size, error rate)
- Alerts on consumer lag > threshold
- Dashboard showing ingestion rate

### Error Recovery

Current: Logs errors, skips failed batches
Future: Implement Dead Letter Queue (DLQ) for failed events

## Next Steps

After Bronze Layer is working:

1. **Stage 4**: Flink real-time processing (Bronze → Silver)
2. **Stage 5**: Spark + Airflow batch ETL (Silver → Gold)
3. **Stage 6**: Redis billing + Analytics dashboard

---

**Status**: ✅ Implementation complete, ready for testing
