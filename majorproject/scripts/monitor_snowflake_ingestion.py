#!/usr/bin/env python3
"""
Real-time Snowflake Bronze Layer Monitor
Shows events flowing into Snowflake with refresh every N seconds
"""
import snowflake.connector
import os
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def clear_screen():
    """Clear terminal screen"""
    os.system('clear' if os.name != 'nt' else 'cls')

def connect_snowflake():
    """Connect to Snowflake"""
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse='COMPUTE_WH',
        database='FLOWGUARD_DB',
        schema='BRONZE'
    )

def get_recent_stats(cursor, minutes=5):
    """Get ingestion stats for last N minutes"""
    stats = {}
    
    # Orders count
    cursor.execute(f"""
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT USER_ID) as unique_users,
            SUM(PRICE) as total_revenue,
            MIN(INGESTION_TIMESTAMP) as first_event,
            MAX(INGESTION_TIMESTAMP) as last_event
        FROM ORDERS_RAW
        WHERE INGESTION_TIMESTAMP >= DATEADD(minute, -{minutes}, CURRENT_TIMESTAMP())
    """)
    row = cursor.fetchone()
    stats['orders'] = {
        'total': row[0],
        'unique_users': row[1],
        'revenue': float(row[2]) if row[2] else 0,
        'first_event': row[3],
        'last_event': row[4]
    }
    
    # Clicks count
    cursor.execute(f"""
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT USER_ID) as unique_users,
            SUM(CASE WHEN EVENT_TYPE = 'click' THEN 1 ELSE 0 END) as clicks,
            SUM(CASE WHEN EVENT_TYPE = 'impression' THEN 1 ELSE 0 END) as impressions,
            MIN(INGESTION_TIMESTAMP) as first_event,
            MAX(INGESTION_TIMESTAMP) as last_event
        FROM CLICKS_RAW
        WHERE INGESTION_TIMESTAMP >= DATEADD(minute, -{minutes}, CURRENT_TIMESTAMP())
    """)
    row = cursor.fetchone()
    stats['clicks'] = {
        'total': row[0],
        'unique_users': row[1],
        'clicks': row[2],
        'impressions': row[3],
        'first_event': row[4],
        'last_event': row[5]
    }
    
    # Total counts
    cursor.execute("SELECT COUNT(*) FROM ORDERS_RAW")
    stats['orders_all_time'] = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM CLICKS_RAW")
    stats['clicks_all_time'] = cursor.fetchone()[0]
    
    return stats

def get_latest_events(cursor, limit=5):
    """Get latest events from both tables"""
    # Latest orders
    cursor.execute(f"""
        SELECT 
            EVENT_ID,
            USER_ID,
            ITEM_NAME,
            PRICE,
            STATUS,
            INGESTION_TIMESTAMP
        FROM ORDERS_RAW
        ORDER BY INGESTION_TIMESTAMP DESC
        LIMIT {limit}
    """)
    orders = cursor.fetchall()
    
    # Latest clicks
    cursor.execute(f"""
        SELECT 
            EVENT_ID,
            USER_ID,
            EVENT_TYPE,
            ITEM_ID,
            INGESTION_TIMESTAMP
        FROM CLICKS_RAW
        ORDER BY INGESTION_TIMESTAMP DESC
        LIMIT {limit}
    """)
    clicks = cursor.fetchall()
    
    return orders, clicks

def format_timestamp(ts):
    """Format timestamp for display"""
    if ts is None:
        return "N/A"
    if isinstance(ts, str):
        return ts
    return ts.strftime("%H:%M:%S")

def monitor_ingestion(refresh_seconds=3, window_minutes=5):
    """Monitor Snowflake ingestion in real-time"""
    conn = connect_snowflake()
    cursor = conn.cursor()
    
    print("=" * 100)
    print("🔍 FlowGuard Bronze Layer - Real-Time Ingestion Monitor")
    print(f"   Refresh: every {refresh_seconds}s | Window: last {window_minutes} minutes")
    print(f"   Connected to: FLOWGUARD_DB.BRONZE")
    print("=" * 100)
    
    iteration = 0
    prev_orders = 0
    prev_clicks = 0
    
    try:
        while True:
            iteration += 1
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Get stats
            stats = get_recent_stats(cursor, window_minutes)
            orders, clicks = get_latest_events(cursor, limit=3)
            
            # Calculate rate
            orders_delta = stats['orders']['total'] - prev_orders
            clicks_delta = stats['clicks']['total'] - prev_clicks
            prev_orders = stats['orders']['total']
            prev_clicks = stats['clicks']['total']
            
            # Clear and display
            clear_screen()
            print("=" * 100)
            print(f"🔍 FlowGuard Bronze Layer Monitor | {current_time} | Refresh #{iteration}")
            print("=" * 100)
            
            # Orders section
            print(f"\n📦 ORDERS (Last {window_minutes} min)")
            print(f"   Total: {stats['orders']['total']} events | Unique Users: {stats['orders']['unique_users']} | Revenue: ${stats['orders']['revenue']:.2f}")
            if stats['orders']['last_event']:
                print(f"   Latest: {format_timestamp(stats['orders']['last_event'])} | Rate: +{orders_delta} events/{refresh_seconds}s")
            print(f"   All-time: {stats['orders_all_time']} orders")
            
            if orders:
                print("\n   Latest Orders:")
                for order in orders[:3]:
                    event_id, user_id, item_name, price, status, ts = order
                    print(f"     • [{format_timestamp(ts)}] User {user_id} → {item_name[:25]} | ${price} | {event_id[:12]}")
            
            # Clicks section
            print(f"\n👁️  CLICKS/IMPRESSIONS (Last {window_minutes} min)")
            print(f"   Total: {stats['clicks']['total']} events | Clicks: {stats['clicks']['clicks']} | Impressions: {stats['clicks']['impressions']}")
            print(f"   Unique Users: {stats['clicks']['unique_users']}")
            if stats['clicks']['last_event']:
                print(f"   Latest: {format_timestamp(stats['clicks']['last_event'])} | Rate: +{clicks_delta} events/{refresh_seconds}s")
            print(f"   All-time: {stats['clicks_all_time']} clicks")
            
            if clicks:
                print("\n   Latest Clicks:")
                for click in clicks[:3]:
                    event_id, user_id, event_type, item_id, ts = click
                    print(f"     • [{format_timestamp(ts)}] User {user_id} {event_type} item #{item_id} | {event_id[:12]}")
            
            # System health
            print(f"\n📊 INGESTION HEALTH")
            if orders_delta > 0 or clicks_delta > 0:
                print(f"   ✅ ACTIVE - Processing events ({orders_delta} orders, {clicks_delta} clicks in last {refresh_seconds}s)")
            else:
                print(f"   ⏸️  IDLE - No new events in last {refresh_seconds}s")
            
            print("\n" + "=" * 100)
            print(f"Press Ctrl+C to stop monitoring | Refreshing in {refresh_seconds}s...")
            
            time.sleep(refresh_seconds)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Monitor stopped")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    try:
        monitor_ingestion(refresh_seconds=3, window_minutes=5)
    except Exception as e:
        print(f"❌ Monitor failed: {e}")
        import traceback
        traceback.print_exc()
