import os
import time
import psycopg2
import redis
from prometheus_client import Gauge, start_http_server

ANALYTICS_POSTGRES_HOST = os.getenv("ANALYTICS_POSTGRES_HOST", "flowguard-analytics")
ANALYTICS_POSTGRES_PORT = int(os.getenv("ANALYTICS_POSTGRES_PORT", "5432"))
ANALYTICS_POSTGRES_USER = os.getenv("ANALYTICS_POSTGRES_USER", "flowguard")
ANALYTICS_POSTGRES_PASSWORD = os.getenv("ANALYTICS_POSTGRES_PASSWORD", "flowguard123")
ANALYTICS_POSTGRES_DB = os.getenv("ANALYTICS_POSTGRES_DB", "flowguard_analytics")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

g_bronze_orders = Gauge("flowguard_bronze_orders_rows", "Rows in analytics.bronze_orders_raw")
g_bronze_clicks = Gauge("flowguard_bronze_clicks_rows", "Rows in analytics.bronze_clicks_raw")
g_silver_orders = Gauge("flowguard_silver_orders_rows", "Rows in analytics.silver_orders_clean")
g_silver_clicks = Gauge("flowguard_silver_clicks_rows", "Rows in analytics.silver_clicks_clean")
g_gold_gmv = Gauge("flowguard_gold_gmv_rows", "Rows in analytics.gold_daily_gmv_metrics")
g_gold_ads = Gauge("flowguard_gold_ads_rows", "Rows in analytics.gold_ads_attribution")
g_recon_rows = Gauge("flowguard_recon_rows", "Rows in analytics.recon_hourly_totals")

g_fraud_total = Gauge("flowguard_fraud_alerts_total", "Fraud alerts total from Redis")
g_fraud_recent_len = Gauge("flowguard_fraud_recent_list_len", "fraud:alerts:recent list length")
g_recon_overwrite_total = Gauge("flowguard_recon_overwrite_total", "recon:last_overwrite_total from Redis")

g_exporter_up = Gauge("flowguard_exporter_up", "1 when exporter loop runs successfully")


def query_count(cur, table_name):
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return int(cur.fetchone()[0])


def collect_postgres():
    conn = psycopg2.connect(
        host=ANALYTICS_POSTGRES_HOST,
        port=ANALYTICS_POSTGRES_PORT,
        user=ANALYTICS_POSTGRES_USER,
        password=ANALYTICS_POSTGRES_PASSWORD,
        dbname=ANALYTICS_POSTGRES_DB,
    )
    cur = conn.cursor()
    try:
        g_bronze_orders.set(query_count(cur, "analytics.bronze_orders_raw"))
        g_bronze_clicks.set(query_count(cur, "analytics.bronze_clicks_raw"))
        g_silver_orders.set(query_count(cur, "analytics.silver_orders_clean"))
        g_silver_clicks.set(query_count(cur, "analytics.silver_clicks_clean"))
        g_gold_gmv.set(query_count(cur, "analytics.gold_daily_gmv_metrics"))
        g_gold_ads.set(query_count(cur, "analytics.gold_ads_attribution"))
        g_recon_rows.set(query_count(cur, "analytics.recon_hourly_totals"))
    finally:
        cur.close()
        conn.close()


def collect_redis():
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_connect_timeout=2)
    g_fraud_total.set(int(client.get("metrics:fraud_alerts_total") or 0))
    g_fraud_recent_len.set(int(client.llen("fraud:alerts:recent") or 0))
    g_recon_overwrite_total.set(int(client.get("recon:last_overwrite_total") or 0))


def main():
    start_http_server(9115)
    while True:
        ok = 1
        try:
            collect_postgres()
            collect_redis()
        except Exception:
            ok = 0
        g_exporter_up.set(ok)
        time.sleep(10)


if __name__ == "__main__":
    main()
