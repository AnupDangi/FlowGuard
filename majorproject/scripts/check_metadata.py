"""Quick check for metadata columns in Snowflake"""
import snowflake.connector
import os
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

print('📊 ORDERS_RAW - Recent metadata check (last 5):')
print('-' * 80)
cur.execute('''
    SELECT EVENT_ID, ORDER_ID, EVENT_TYPE, SOURCE_TOPIC, INGESTION_ID, 
           TO_CHAR(INGESTION_TIMESTAMP, 'HH24:MI:SS')
    FROM ORDERS_RAW 
    ORDER BY INGESTION_TIMESTAMP DESC 
    LIMIT 5
''')
for row in cur.fetchall():
    evt_id = row[0][:15] if row[0] else 'NULL'
    order_id = row[1][:20] if row[1] else 'NULL'
    event_type = row[2] or 'NULL'
    source_topic = row[3] or 'NULL'
    ingestion_id = (row[4][:40] + '...') if row[4] and len(row[4]) > 40 else (row[4] or 'NULL')
    time = row[5] or 'NULL'
    print(f'  {time} | {evt_id:15} | Type: {event_type:10} | Topic: {source_topic:20} | Ingestion: {ingestion_id}')

print('\n📊 CLICKS_RAW - Recent metadata check (last 5):')
print('-' * 80)
cur.execute('''
    SELECT EVENT_ID, ITEM_ID, SESSION_ID, SOURCE_TOPIC, INGESTION_ID,
           TO_CHAR(INGESTION_TIMESTAMP, 'HH24:MI:SS')
    FROM CLICKS_RAW 
    ORDER BY INGESTION_TIMESTAMP DESC 
    LIMIT 5
''')
for row in cur.fetchall():
    evt_id = row[0][:15] if row[0] else 'NULL'
    item_id = str(row[1]) if row[1] else 'NULL'
    session_id = (row[2][:20] + '...') if row[2] and len(row[2]) > 20 else (row[2] or 'NULL')
    source_topic = row[3] or 'NULL'
    ingestion_id = (row[4][:40] + '...') if row[4] and len(row[4]) > 40 else (row[4] or 'NULL')
    time = row[5] or 'NULL'
    print(f'  {time} | {evt_id:15} | Item: {item_id:5} | Session: {session_id:23} | Topic: {source_topic:20} | Ingestion: {ingestion_id}')

# Check for NULL metadata
print('\n🔍 NULL metadata analysis:')
cur.execute('SELECT COUNT(*) as total, COUNT(INGESTION_ID) as with_ingestion_id FROM ORDERS_RAW')
res = cur.fetchone()
print(f'  ORDERS: {res[0]} total, {res[1]} with INGESTION_ID, {res[0] - res[1]} without')

cur.execute('SELECT COUNT(*) as total, COUNT(INGESTION_ID) as with_ingestion_id FROM CLICKS_RAW')
res = cur.fetchone()
print(f'  CLICKS: {res[0]} total, {res[1]} with INGESTION_ID, {res[0] - res[1]} without')

cur.close()
conn.close()
