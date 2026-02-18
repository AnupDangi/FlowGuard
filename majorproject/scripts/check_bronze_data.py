#!/usr/bin/env python3
"""Check Bronze data in Snowflake"""
import snowflake.connector
import os
import sys

# Load environment
sys.path.insert(0, os.path.dirname(__file__))
with open('.env') as f:
    for line in f:
        if line.strip() and not line.startswith('#') and '=' in line:
            key, val = line.strip().split('=', 1)
            os.environ[key] = val

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse='COMPUTE_WH',
    database='FLOWGUARD_DB'
)

cursor = conn.cursor()

print('\n=== BRONZE LAYER DATA ===')
cursor.execute('SELECT COUNT(*), MIN(ORDER_TIMESTAMP), MAX(ORDER_TIMESTAMP) FROM BRONZE.ORDERS_RAW')
orders = cursor.fetchone()
print(f'Orders: {orders[0]} rows')
if orders[1]:
    print(f'Date range: {orders[1]} to {orders[2]}')

cursor.execute('SELECT COUNT(*), MIN(CLICK_TIMESTAMP), MAX(CLICK_TIMESTAMP) FROM BRONZE.CLICKS_RAW')
clicks = cursor.fetchone()
print(f'\nClicks: {clicks[0]} rows')
if clicks[1]:
    print(f'Date range: {clicks[1]} to {clicks[2]}')

cursor.execute('SELECT DISTINCT DATE_PARTITION FROM BRONZE.ORDERS_RAW ORDER BY DATE_PARTITION DESC LIMIT 10')
dates = cursor.fetchall()
print(f'\nAvailable date partitions:')
for d in dates:
    print(f'  - {d[0]}')

cursor.close()
conn.close()
