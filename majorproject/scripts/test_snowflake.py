#!/usr/bin/env python3
"""Test Snowflake connection and initialize schema"""

import os
import sys

# Add project root to path
project_root = '/Users/anupdangi/Desktop/AnupAI/projects/2026/FlowGuard/majorproject'
sys.path.insert(0, project_root)

# Load environment variables from .env
from dotenv import load_dotenv
load_dotenv(os.path.join(project_root, '.env'))

import snowflake.connector

def main():
    # Get credentials from environment
    password = os.getenv('SNOWFLAKE_PASSWORD')
    user = os.getenv('SNOWFLAKE_USER')
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    
    if not password:
        print("❌ Error: SNOWFLAKE_PASSWORD not found in .env file")
        sys.exit(1)
    
    print("📡 Connecting to Snowflake...")
    print(f"   Account: {account}")
    print(f"   User: {user}")
    
    try:
        # Connect using environment variables
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'FLOWGUARD_DB'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'BRONZE')
        )
        
        print("✅ Connected successfully!")
        
        # Read SQL file
        sql_file = '/Users/anupdangi/Desktop/AnupAI/projects/2026/FlowGuard/majorproject/scripts/snowflake/init_bronze_schema.sql'
        print(f"\n📄 Executing {sql_file}...")
        
        with open(sql_file, 'r') as f:
            sql = f.read()
        
        # Execute statements one by one
        cursor = conn.cursor()
        
        # Split by semicolon and clean
        raw_statements = sql.split(';')
        statements = []
        for s in raw_statements:
            s = s.strip()
            # Remove comments
            lines = [line for line in s.split('\n') if not line.strip().startswith('--')]
            s = '\n'.join(lines).strip()
            if s:
                statements.append(s)
        
        for i, statement in enumerate(statements, 1):
            try:
                print(f"[{i}/{len(statements)}] {statement[:60]}...")
                cursor.execute(statement)
                print(f"    ✅ Success")
            except Exception as e:
                error_msg = str(e).lower()
                if 'already exists' in error_msg or 'does not exist' in error_msg:
                    print(f"    ⚠️  {e}")
                else:
                    print(f"    ❌ Error: {e}")
        
        # Verify tables
        print("\n� Verifying tables...")
        cursor.execute("SHOW TABLES IN BRONZE")
        tables = cursor.fetchall()
        for table in tables:
            print(f"   ✅ {table[1]}")
        
        cursor.close()
        conn.close()
        
        print("\n✅ Snowflake Bronze schema initialized!")
        print("Tables: ORDERS_RAW, CLICKS_RAW")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
