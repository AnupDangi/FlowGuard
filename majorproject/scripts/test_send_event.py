"""Test script to send sample order events."""
import requests
import json
from datetime import datetime
import time

EVENTS_GATEWAY = "http://localhost:8000"
FOOD_CATALOG = "http://localhost:8001"


def send_test_order():
    """Send a test order event."""
    # Get a random food item
    response = requests.get(f"{FOOD_CATALOG}/api/foods")
    foods = response.json()['items']
    food = foods[0]  # Get first food
    
    # Create order event
    order_event = {
        "user_id": "user_test_123",
        "order_id": f"order_{int(time.time())}",
        "item_id": food['food_id'],
        "item_name": food['name'],
        "price": food['price'],
        "timestamp": datetime.utcnow().isoformat()
    }
    
    print(f"📤 Sending order event:")
    print(json.dumps(order_event, indent=2))
    
    # Send to Events Gateway
    response = requests.post(
        f"{EVENTS_GATEWAY}/api/v1/orders",
        json=order_event
    )
    
    if response.status_code == 202:
        print(f"\n✅ Order event accepted!")
        print(json.dumps(response.json(), indent=2))
    else:
        print(f"\n❌ Failed: {response.status_code}")
        print(response.text)


if __name__ == "__main__":
    print("="*60)
    print("FlowGuard Event Test")
    print("="*60)
    print("\nSending test order event to Events Gateway...")
    print("Watch the monitor terminal to see the event!\n")
    
    send_test_order()
    
    print("\n" + "="*60)
    print("Check your event monitor terminal!")
    print("="*60)
