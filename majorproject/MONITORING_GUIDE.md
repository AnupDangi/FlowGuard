# FlowGuard Monitoring & Observability Guide

## 🎯 Real-Time Monitoring Setup

### 1. **Monitor Kafka Events** (Real-time Event Stream)

Watch all orders and clicks flowing through Kafka in real-time with colored output:

```bash
python scripts/monitor_events.py
```

**What you'll see:**

- 🟢 **Green events** = Order events (raw.orders.v1 topic)
- 🔵 **Blue events** = Click/Impression events (raw.clicks.v1 topic)
- Event details: user_id, order_id, item details, price, timestamps

**Example output:**

```
🟢 ORDER EVENT | user_id: 1234567 | order_id: order_1738868400123_abc123
   Item: Chicken Biryani (ID: 1) | Price: ₹299.0 | Time: 14:30:25
```

### 2. **Monitor Service Logs** (API Activity)

Monitor both Food Catalog API (8001) and Events Gateway (8000):

```bash
./scripts/monitor_services.sh
```

**What you'll see:**

- API requests and responses
- Database queries
- Kafka producer confirmations
- Error messages

### 3. **Docker Infrastructure Logs**

**All services together:**

```bash
docker-compose logs -f
```

**Specific services:**

```bash
# PostgreSQL logs
docker logs flowguard-postgres -f

# Kafka broker logs
docker logs kafka-broker-1 -f
docker logs kafka-broker-2 -f
docker logs kafka-broker-3 -f

# Kafka UI logs
docker logs kafka-ui -f
```

### 4. **Kafka UI** (Visual Dashboard)

Open your browser to see topics, partitions, messages, and consumer groups:

```
http://localhost:8080
```

**Features:**

- Browse Kafka topics
- View message contents
- Monitor consumer lag
- Check broker health

## 📊 Production-Level Monitoring Stack

### Current Setup

```
┌─────────────────────────────────────────────────┐
│  Next.js UI (localhost:3000)                    │
│  - User interactions                            │
│  - Event tracking (impressions, clicks, orders) │
└─────────────────┬───────────────────────────────┘
                  │
                  ↓
┌─────────────────────────────────────────────────┐
│  Food Catalog API (localhost:8001)              │
│  - PostgreSQL reference data                    │
│  - Food items with descriptions                 │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│  Events Gateway (localhost:8000)                │
│  - POST /api/v1/orders/ (order events)          │
│  - POST /api/v1/clicks/ (click/impression)      │
└─────────────────┬───────────────────────────────┘
                  │
                  ↓
┌─────────────────────────────────────────────────┐
│  Kafka Cluster (3 brokers)                      │
│  - Broker 1: localhost:19092                    │
│  - Broker 2: localhost:19093                    │
│  - Broker 3: localhost:19094                    │
│                                                  │
│  Topics:                                         │
│  - raw.orders.v1 (3 partitions, RF=2)           │
│  - raw.clicks.v1 (6 partitions, RF=2)           │
└─────────────────┬───────────────────────────────┘
                  │
                  ↓
┌─────────────────────────────────────────────────┐
│  Monitor Scripts                                 │
│  - scripts/monitor_events.py (colored output)   │
│  - scripts/monitor_services.sh (service logs)   │
└─────────────────────────────────────────────────┘
```

## 🚀 Testing the Complete Flow

### Step 1: Start All Services

```bash
cd /Users/anupdangi/Desktop/AnupAI/projects/2026/FlowGuard/majorproject
./scripts/start_all.sh
```

### Step 2: Open Event Monitor (Terminal 1)

```bash
python scripts/monitor_events.py
```

Keep this running to see all events in real-time.

### Step 3: Start Next.js UI (Terminal 2)

```bash
cd zomato-web-app
npm run dev
```

### Step 4: Open the UI

```
http://localhost:3000
```

### Step 5: Test Event Tracking

**Test Impression Tracking:**

1. Hover over any food card
2. Watch monitor_events.py → Blue click event with `is_click: false`

**Test Click Tracking:**

1. Click on a food card (not the button)
2. Watch monitor_events.py → Blue click event with `is_click: true`

**Test Order Tracking:**

1. Click "Order Now" button on any food
2. Watch monitor_events.py → Green order event
3. Browser navigates to `/order/{orderId}` page
4. See order details with description and unique order_id

## 🔍 Event Structure

### Order Event

```json
{
  "event_id": "evt_1738868400123_abc123",
  "user_id": 1234567890,
  "order_id": "order_1738868400123_def456",
  "item_id": 1,
  "item_name": "Chicken Biryani",
  "price": 299.0,
  "timestamp": "2026-02-06T14:30:25.123Z"
}
```

### Click/Impression Event

```json
{
  "event_id": "evt_1738868400456_ghi789",
  "user_id": 1234567890,
  "ad_id": "food_1",
  "is_click": true,
  "timestamp": "2026-02-06T14:30:20.456Z"
}
```

## 📈 Key Metrics to Watch

1. **Event Latency**: Time from UI action to Kafka
2. **Event Volume**: Orders/clicks per minute
3. **Error Rate**: Failed API calls
4. **Kafka Lag**: Consumer group lag (future Stage 3)
5. **Database Queries**: Food Catalog API performance

## 🎯 Next Steps (Stage 3)

- **Snowflake Bronze Layer**: Consume Kafka → Write to Snowflake
- **Flink Attribution**: Correlate clicks with orders (30-min window)
- **Spark Batch ETL**: Silver/Gold layers with Airflow
- **Analytics Dashboard**: Conversion metrics, top foods, user behavior

## 🛠️ Troubleshooting

**No events appearing in monitor?**

```bash
# Check Events Gateway is running
curl http://localhost:8000/health

# Check Kafka is running
docker ps | grep kafka

# Check Next.js dev server
curl http://localhost:3000
```

**Order page not loading?**

- Check browser console for errors
- Verify localStorage has `order_{orderId}` entry
- Check if Food Catalog API returned description field

**Service logs location:**

- Food Catalog: `/tmp/catalog.log`
- Events Gateway: `/tmp/events_gateway.log`
