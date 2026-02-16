#!/usr/bin/env python3
"""
Generate continuous test traffic to Events Gateway API
Simulates real user activity: orders and clicks
"""
import requests
import random
import time
from datetime import datetime

EVENTS_GATEWAY = "http://localhost:8000"

# Sample food items (matching catalog)
FOOD_ITEMS = [
    {"id": 1, "name": "Margherita Pizza", "price": 12.99},
    {"id": 2, "name": "Pepperoni Pizza", "price": 14.99},
    {"id": 3, "name": "Chicken Burger", "price": 8.99},
    {"id": 4, "name": "Veg Burger", "price": 7.99},
    {"id": 5, "name": "Caesar Salad", "price": 9.99},
]

USER_IDS = [101, 102, 103, 104, 105, 106, 107, 108, 109, 110]
SESSION_IDS = [f"sess_{i}" for i in range(1, 6)]

def send_order(user_id, item):
    """Send order event"""
    payload = {
        "user_id": user_id,
        "item_id": item["id"],
        "item_name": item["name"],
        "price": item["price"]
    }
    try:
        resp = requests.post(f"{EVENTS_GATEWAY}/api/v1/orders/", json=payload, timeout=2)
        if resp.status_code == 200:
            data = resp.json()
            print(f"✅ ORDER: user={user_id} item={item['name'][:20]} price=${item['price']} order_id={data.get('order_id', 'N/A')[:8]}")
            return True
        else:
            print(f"❌ ORDER FAILED: {resp.status_code}")
            return False
    except Exception as e:
        print(f"❌ ORDER ERROR: {e}")
        return False

def send_click(user_id, item, event_type="click"):
    """Send click/impression event"""
    payload = {
        "user_id": user_id,
        "event_type": event_type,
        "item_id": item["id"],
        "session_id": random.choice(SESSION_IDS)
    }
    try:
        resp = requests.post(f"{EVENTS_GATEWAY}/api/v1/events/track", json=payload, timeout=2)
        if resp.status_code == 200:
            data = resp.json()
            print(f"  👁️  {event_type.upper()}: user={user_id} item={item['name'][:20]} event_id={data.get('event_id', 'N/A')[:16]}")
            return True
        else:
            print(f"  ❌ {event_type.upper()} FAILED: {resp.status_code}")
            return False
    except Exception as e:
        print(f"  ❌ {event_type.upper()} ERROR: {e}")
        return False

def generate_traffic(events_per_cycle=5, delay_seconds=3):
    """Generate continuous traffic"""
    print("=" * 80)
    print("🚀 FlowGuard Traffic Generator Started")
    print(f"   Events per cycle: {events_per_cycle}")
    print(f"   Delay: {delay_seconds}s")
    print(f"   Target: {EVENTS_GATEWAY}")
    print("=" * 80)
    
    cycle = 0
    total_orders = 0
    total_clicks = 0
    
    try:
        while True:
            cycle += 1
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"\n[{timestamp}] Cycle #{cycle} - Generating {events_per_cycle} events...")
            
            for _ in range(events_per_cycle):
                user = random.choice(USER_IDS)
                item = random.choice(FOOD_ITEMS)
                
                # 70% clicks, 30% orders (realistic traffic pattern)
                if random.random() < 0.7:
                    # Send 2-3 clicks before potential order
                    impressions = random.randint(2, 3)
                    for _ in range(impressions):
                        if send_click(user, item, "impression"):
                            total_clicks += 1
                        time.sleep(0.2)
                    
                    if send_click(user, item, "click"):
                        total_clicks += 1
                        time.sleep(0.3)
                else:
                    # User places order
                    if send_order(user, item):
                        total_orders += 1
                
                time.sleep(0.5)  # Space out events
            
            print(f"📊 Total: {total_orders} orders, {total_clicks} clicks")
            time.sleep(delay_seconds)
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Traffic generator stopped")
        print(f"📊 Final stats: {total_orders} orders, {total_clicks} clicks")

if __name__ == "__main__":
    # Quick sanity check
    try:
        resp = requests.get(f"http://localhost:8001/health", timeout=2)
        print("✅ Food Catalog API: Online")
    except:
        print("⚠️  Food Catalog API: Offline")
    
    try:
        resp = requests.get(f"{EVENTS_GATEWAY}/health", timeout=2)
        print("✅ Events Gateway API: Online")
    except:
        print("❌ Events Gateway API: OFFLINE - Start it first!")
        exit(1)
    
    # Start generating
    generate_traffic(events_per_cycle=3, delay_seconds=5)
