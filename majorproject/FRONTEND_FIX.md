# Frontend Connection Fix - Summary

## 🐛 Problem Identified

**Frontend couldn't load food items** because backend services were connecting to the wrong PostgreSQL port.

### Root Cause:
When we changed PostgreSQL from port 5432 → 5433 (to avoid conflict with Airflow), the backend services were not updated and were trying to connect to the old port 5432 (which is now Airflow's metadata DB).

---

## ✅ What Was Fixed

### 1. **Database Port Configuration**
- **Added PostgreSQL config to `.env`:**
  ```bash
  POSTGRES_HOST=localhost
  POSTGRES_PORT=5433  # Updated from 5432
  POSTGRES_USER=flowguard
  POSTGRES_PASSWORD=flowguard123
  POSTGRES_DB=food_catalog
  ```

### 2. **Service Startup Process**
- **Created `scripts/start_services.sh`:**
  - Loads `.env` file before starting services
  - Properly activates virtual environment
  - Provides health checks after startup
  - Shows log file locations for debugging

- **Updated `scripts/start_all.sh`:**
  - Now calls `start_services.sh` to start backend
  - Fixed PostgreSQL port display (5433)
  - Better error messages

### 3. **Restarted Services**
- Food Catalog now connects to PostgreSQL on port 5433 ✅
- Events Gateway also updated with correct config ✅
- Both services verified as healthy ✅

---

## 🎯 Current System Status

### **Infrastructure (Docker)**
| Service | Port | Status |
|---------|------|--------|
| Zookeeper | 2181 | ✅ Healthy |
| Kafka Broker 1 | 19092 | ✅ Healthy |
| Kafka Broker 2 | 19093 | ✅ Healthy |
| Kafka Broker 3 | 19094 | ✅ Healthy |
| Kafka UI | 8081 | ✅ Running |
| PostgreSQL (Food) | **5433** | ✅ Healthy |
| PostgreSQL (Airflow) | 5432 | ✅ Running |
| Airflow UI | 8080 | ✅ Running |

### **Backend Services**
| Service | Port | Status | Database | Kafka |
|---------|------|--------|----------|-------|
| Food Catalog | 8001 | ✅ Healthy | ✅ Connected | N/A |
| Events Gateway | 8000 | ✅ Healthy | ✅ Connected | ✅ Connected |

### **API Endpoints Verified**
```bash
# Food Catalog
✅ GET http://localhost:8001/api/foods (25 items)
✅ GET http://localhost:8001/api/foods/categories/list (9 categories)
✅ GET http://localhost:8001/api/foods?category=Biryani (3 items)

# Events Gateway
✅ GET http://localhost:8000/health
✅ POST http://localhost:8000/api/v1/orders
✅ POST http://localhost:8000/api/v1/clicks
```

---

## 🚀 Test Frontend Now

### 1. **Start Web App**
```bash
cd zomato-web-app
pnpm dev
```

### 2. **Open in Browser**
```
http://localhost:3000
```

### 3. **Expected Behavior**
- ✅ Food categories load in navbar
- ✅ Food items display with images and prices
- ✅ Can filter by category (Biryani, Pizza, etc.)
- ✅ Can add items to cart
- ✅ Can place orders (tracked to Kafka → Snowflake)

### 4. **If Issues Occur**

**Check backend health:**
```bash
curl http://localhost:8001/health
curl http://localhost:8000/health
```

**Restart services:**
```bash
cd majorproject
./scripts/start_services.sh
```

**View logs:**
```bash
tail -f /tmp/food_catalog.log /tmp/events_gateway.log
```

---

## 📝 Future Startups

**To start everything fresh:**
```bash
cd majorproject

# Start infrastructure + backend services
./scripts/start_all.sh

# Start web UI (separate terminal)
cd zomato-web-app
pnpm dev
```

The `start_all.sh` script now:
1. Starts Docker services (Kafka, PostgreSQL, Zookeeper)
2. Waits for health checks
3. **Automatically loads .env and starts backend services**
4. Verifies all services are healthy

---

## 🔧 Configuration Files

**`.env` (majorproject/.env)** - Backend configuration
- ✅ PostgreSQL connection (port 5433)
- ✅ Kafka brokers
- ✅ Snowflake credentials

**`docker-compose.yml`** - Infrastructure
- ✅ PostgreSQL on port 5433 (was 5432)
- ✅ Kafka UI on port 8081 (was 8080)
- ✅ Kafka brokers with restart policies

**`airflow/docker-compose.override.yml`** - Removed (no longer needed)

---

## 📊 Port Allocations Reference

| Port | Service | Purpose |
|------|---------|---------|
| 3000 | Next.js | Frontend web UI |
| 5433 | PostgreSQL | Food catalog database |
| 5432 | PostgreSQL | Airflow metadata database |
| 8000 | Events Gateway | Order/click ingestion |
| 8001 | Food Catalog | Food items API |
| 8080 | Airflow | ETL orchestration UI |
| 8081 | Kafka UI | Kafka cluster monitoring |
| 2181 | Zookeeper | Kafka coordination |
| 19092-19094 | Kafka | Message brokers (3 nodes) |

---

## ✅ Summary

**Problem:** Frontend couldn't fetch data (port mismatch)  
**Root Cause:** PostgreSQL port changed but services not updated  
**Solution:** Added `.env` config, updated startup scripts, restarted services  
**Status:** All systems operational ✅  
**Next Step:** Test frontend at http://localhost:3000  

---

**Last Updated:** 2026-02-18  
**Git Commits:** a2919fe (startup improvements)
