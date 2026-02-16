#!/usr/bin/env python3
import snowflake.connector, os
from dotenv import load_dotenv
from datetime import datetime

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

# Total counts
cur.execute('SELECT COUNT(*) FROM ORDERS_RAW')
total_orders = cur.fetchone()[0]
cur.execute('SELECT COUNT(*) FROM CLICKS_RAW')
total_clicks = cur.fetchone()[0]

print(f'\n📊 Snowflake Bronze Layer ({datetime.now().strftime("%H:%M:%S")})')
print(f'   Total: {total_orders} orders, {total_clicks} clicks')

# Recent (last minute)
cur.execute("SELECT COUNT(*) FROM ORDERS_RAW WHERE INGESTION_TIMESTAMP >= DATEADD(minute, -1, CURRENT_TIMESTAMP())")
recent_orders = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM CLICKS_RAW WHERE INGESTION_TIMESTAMP >= DATEADD(minute, -1, CURRENT_TIMESTAMP())")
recent_clicks = cur.fetchone()[0]
print(f'   Last 1 min: {recent_orders} orders, {recent_clicks} clicks')

# Latest events
cur.execute('SELECT USER_ID, ITEM_NAME, PRICE, INGESTION_TIMESTAMP FROM ORDERS_RAW ORDER BY INGESTION_TIMESTAMP DESC LIMIT 3')
print(f'\n   Latest Orders:')
for row in cur.fetchall():
    print(f'     • User {row[0]}: {row[1]} ${row[2]} @ {row[3]}')

cur.close()
conn.close()
