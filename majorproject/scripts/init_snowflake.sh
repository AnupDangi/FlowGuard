#!/bin/bash
# Initialize Snowflake Bronze Schema
# Run this once to create tables

set -e

echo "🏗️  Initializing Snowflake Bronze Schema..."
echo "==========================================="

# Check if snowsql is installed
if ! command -v snowsql &> /dev/null; then
    echo "⚠️  snowsql not found. Using Python snowflake-connector instead..."
    
    # Use Python to execute SQL
    python3 << 'EOF'
import os
import sys
import snowflake.connector

# Get password from environment
password = os.getenv('SNOWFLAKE_PASSWORD')
if not password:
    print("❌ Error: SNOWFLAKE_PASSWORD environment variable not set")
    sys.exit(1)

print("📡 Connecting to Snowflake...")

# Connect
conn = snowflake.connector.connect(
    account='ZLNJTCF-KE38237',
    user='ANUPDANGI12',
    password=password,
    warehouse='COMPUTE_WH',
    database='FLOWGUARD_DB',
    schema='BRONZE'
)

print("✅ Connected successfully")

# Read and execute SQL file
sql_file = 'scripts/snowflake/init_bronze_schema.sql'
print(f"📄 Executing {sql_file}...")

with open(sql_file, 'r') as f:
    sql = f.read()

# Execute each statement
cursor = conn.cursor()
for statement in sql.split(';'):
    statement = statement.strip()
    if statement and not statement.startswith('--'):
        try:
            cursor.execute(statement)
            print(f"✅ Executed: {statement[:50]}...")
        except Exception as e:
            # Skip errors for IF NOT EXISTS statements
            if 'already exists' not in str(e):
                print(f"⚠️  Warning: {e}")

cursor.close()
conn.close()

print("✅ Schema initialization complete!")
EOF

else
    # Use snowsql CLI
    echo "📡 Using snowsql CLI..."
    snowsql -a ZLNJTCF-KE38237 -u ANUPDANGI12 -d FLOWGUARD_DB -s BRONZE -f scripts/snowflake/init_bronze_schema.sql
fi

echo ""
echo "✅ Snowflake Bronze schema ready!"
echo "Tables created: ORDERS_RAW, CLICKS_RAW"
