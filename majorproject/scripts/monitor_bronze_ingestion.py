"""Real-time Bronze Layer Ingestion Monitor

Shows events flowing into Snowflake Bronze tables with timestamps.
"""
import snowflake.connector
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    """Get Snowflake connection"""
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse='COMPUTE_WH',
        database='FLOWGUARD_DB',
        schema='BRONZE'
    )

def monitor_ingestion(interval_seconds=5, duration_minutes=5):
    """Monitor Bronze layer ingestion in real-time
    
    Args:
        interval_seconds: How often to poll Snowflake
        duration_minutes: How long to monitor (0 = infinite)
    """
    conn = get_connection()
    cur = conn.cursor()
    
    print("=" * 80)
    print("🔍 BRONZE LAYER INGESTION MONITOR")
    print("=" * 80)
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 Poll interval: {interval_seconds}s")
    print(f"⏳ Duration: {'Infinite' if duration_minutes == 0 else f'{duration_minutes} min'}")
    print("=" * 80)
    print()
    
    start_time = time.time()
    last_orders_count = 0
    last_clicks_count = 0
    
    try:
        while True:
            # Get current timestamp
            now = datetime.now()
            elapsed = time.time() - start_time
            
            # Query recent events (last 5 minutes)
            time_window = now - timedelta(minutes=5)
            time_window_str = time_window.strftime('%Y-%m-%d %H:%M:%S')
            
            # Orders
            cur.execute(f"""
                SELECT 
                    COUNT(*) as total,
                    COUNT(DISTINCT SOURCE_TOPIC) as topics,
                    COUNT(DISTINCT PRODUCER_SERVICE) as services,
                    COUNT(DISTINCT INGESTION_ID) as unique_events,
                    MAX(INGESTION_TIMESTAMP) as last_ingestion
                FROM ORDERS_RAW
                WHERE INGESTION_TIMESTAMP >= '{time_window_str}'
            """)
            orders_stats = cur.fetchone()
            orders_count = orders_stats[0] if orders_stats else 0
            orders_last = orders_stats[4] if orders_stats and orders_stats[4] else 'N/A'
            
            # Clicks
            cur.execute(f"""
                SELECT 
                    COUNT(*) as total,
                    COUNT(DISTINCT SOURCE_TOPIC) as topics,
                    COUNT(DISTINCT PRODUCER_SERVICE) as services,
                    COUNT(DISTINCT INGESTION_ID) as unique_events,
                    MAX(INGESTION_TIMESTAMP) as last_ingestion
                FROM CLICKS_RAW
                WHERE INGESTION_TIMESTAMP >= '{time_window_str}'
            """)
            clicks_stats = cur.fetchone()
            clicks_count = clicks_stats[0] if clicks_stats else 0
            clicks_last = clicks_stats[4] if clicks_stats and clicks_stats[4] else 'N/A'
            
            # Calculate deltas
            orders_delta = orders_count - last_orders_count
            clicks_delta = clicks_count - last_clicks_count
            
            # Display status
            print(f"[{now.strftime('%H:%M:%S')}] Elapsed: {int(elapsed)}s")
            print(f"  📦 ORDERS (last 5min): {orders_count:4d} total  |  +{orders_delta:3d} new  |  Last: {orders_last}")
            print(f"  🖱️  CLICKS (last 5min): {clicks_count:4d} total  |  +{clicks_delta:3d} new  |  Last: {clicks_last}")
            
            # Show recent individual events if any new
            if orders_delta > 0:
                cur.execute(f"""
                    SELECT 
                        EVENT_ID, ORDER_ID, USER_ID, PRICE, 
                        SOURCE_TOPIC, INGESTION_ID, INGESTION_TIMESTAMP
                    FROM ORDERS_RAW
                    WHERE INGESTION_TIMESTAMP >= '{time_window_str}'
                    ORDER BY INGESTION_TIMESTAMP DESC
                    LIMIT 3
                """)
                print(f"\n  🆕 Recent Orders:")
                for row in cur.fetchall():
                    event_id, order_id, user_id, price, topic, ing_id, ing_ts = row
                    print(f"     • {event_id[:20]:<20} | User:{user_id} | ${price} | {ing_ts} | {topic}")
            
            if clicks_delta > 0:
                cur.execute(f"""
                    SELECT 
                        EVENT_ID, EVENT_TYPE, USER_ID, ITEM_ID,
                        SOURCE_TOPIC, INGESTION_ID, INGESTION_TIMESTAMP
                    FROM CLICKS_RAW
                    WHERE INGESTION_TIMESTAMP >= '{time_window_str}'
                    ORDER BY INGESTION_TIMESTAMP DESC
                    LIMIT 3
                """)
                print(f"\n  🆕 Recent Clicks:")
                for row in cur.fetchall():
                    event_id, event_type, user_id, item_id, topic, ing_id, ing_ts = row
                    print(f"     • {event_id[:20]:<20} | {event_type} | User:{user_id} | Item:{item_id} | {ing_ts}")
            
            print()
            print("-" * 80)
            
            # Update counters
            last_orders_count = orders_count
            last_clicks_count = clicks_count
            
            # Check if duration reached
            if duration_minutes > 0 and elapsed >= (duration_minutes * 60):
                print(f"\n✅ Monitoring complete ({duration_minutes} minutes)")
                break
            
            # Wait before next poll
            time.sleep(interval_seconds)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Monitoring stopped by user")
    finally:
        cur.close()
        conn.close()
        
        print("\n" + "=" * 80)
        print(f"⏰ Ended: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

if __name__ == '__main__':
    import sys
    
    # Parse command line args
    interval = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    duration = int(sys.argv[2]) if len(sys.argv) > 2 else 2  # 2 minutes default
    
    monitor_ingestion(interval_seconds=interval, duration_minutes=duration)
