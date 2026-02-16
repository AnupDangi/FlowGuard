"""Test Snowflake V ARIANT insertion methods"""
import snowflake.connector
import os
import json
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse='COMPUTE_WH',
    database='FLOWGUARD_DB',
    schema='BRONZE'
)

cur = conn.cursor()

# Test event
event = {
    'event_id': 'test_variant_001',
    'order_id': 'ord_test_001',
    'user_id': 888,
    'item_id': 1,
    'item_name': 'Test Pizza',
    'price': 19.99,
    'status': 'test',
    'timestamp': '2026-02-07T16:45:00',
    'extra_field': 'should be in VARIANT'
}

# Method 1: Using PARSE_JSON in a SELECT subquery
print("\n1️⃣ Testing PARSE_JSON with SELECT subquery...")
try:
    sql = '''
    INSERT INTO ORDERS_RAW (
        EVENT_ID, ORDER_ID, USER_ID, ITEM_ID, ITEM_NAME,
        PRICE, STATUS, EVENT_TIMESTAMP, RAW_EVENT
    )
    SELECT %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)
    '''
    cur.execute(sql, (
        event['event_id'],
        event['order_id'],
        event['user_id'],
        event['item_id'],
        event['item_name'],
        event['price'],
        event['status'],
        event['timestamp'],
        json.dumps(event)  # JSON string
    ))
    print("✅ WORKED! PARSE_JSON in SELECT works")
    conn.commit()
except Exception as e:
    print(f"❌ Failed: {e}")
    conn.rollback()

# Verify insertion
cur.execute("SELECT * FROM ORDERS_RAW WHERE EVENT_ID = 'test_variant_001'")
row = cur.fetchone()
if row:
    print(f"\n📊 Inserted row found:")
    print(f"  EVENT_ID: {row[0]}")
    print(f"  RAW_EVENT type: {type(row[8])}")
    print(f"  RAW_EVENT content: {row[8]}")

cur.close()
conn.close()
print("\n✅ Test complete!")
