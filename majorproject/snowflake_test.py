import snowflake.connector
import os

# Define connection parameters (use environment variables for security best practice)
# e.g., set SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT environment variables
USER = os.getenv('SNOWFLAKE_USER')
PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT') # e.g., 'myorganization-myaccount' or 'xy12345.us-east-2.aws'
WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'tiny_warehouse_mg') # Optional
DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'testdb_mg') # Optional
SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'testschema_mg') # Optional

# Establish the connection
try:
    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )
    print("Connection successful!")

    # # Create a cursor object
    # cur = conn.cursor()

    # # Execute a SQL statement (e.g., check the current Snowflake version)
    # cur.execute("SELECT current_version()")

    # # Fetch the result
    # one_row = cur.fetchone()
    # print(f"Snowflake Version: {one_row[0]}")

    # # You can also iterate over results for multiple rows
    # # cur.execute("SELECT col1, col2 FROM test_table")
    # # for (col1, col2) in cur:
    # #     print(f"{col1}, {col2}")

except snowflake.connector.errors.ProgrammingError as e:
    print(f"Connection failed: {e}")

finally:
    # # Close the cursor and connection
    # if 'cur' in locals() and cur is not None:
    #     cur.close()
    # if 'conn' in locals() and conn is not None:
    #     conn.close()
    print("Connection closed.")
