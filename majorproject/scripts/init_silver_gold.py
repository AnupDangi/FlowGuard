#!/usr/bin/env python3
"""Initialize Silver and Gold schemas in Snowflake"""
import snowflake.connector
import os

# Load environment variables
env = {}
with open('.env') as f:
    for line in f:
        if line.strip() and not line.startswith('#') and '=' in line:
            k, v = line.strip().split('=', 1)
            env[k] = v

conn = snowflake.connector.connect(
    user=env['SNOWFLAKE_USER'],
    password=env['SNOWFLAKE_PASSWORD'],
    account=env['SNOWFLAKE_ACCOUNT'],
    warehouse='COMPUTE_WH'
)

cursor = conn.cursor()

# Read and execute SQL
with open('scripts/snowflake/init_silver_gold_schema.sql') as f:
    sql_script = f.read()
    
# Execute each statement
for statement in sql_script.split(';'):
    statement = statement.strip()
    if statement and not statement.startswith('--'):
        try:
            cursor.execute(statement)
            print(f'✅ Executed: {statement[:60]}...')
        except Exception as e:
            if 'already exists' not in str(e):
                print(f'⚠️  Error: {e}')

cursor.close()
conn.close()

print('\n✅ Silver and Gold schemas initialized!')
