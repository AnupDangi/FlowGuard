"""
FlowGuard Daily ETL Pipeline - SQL-Based (CORRECTED SCHEMA)

This DAG processes Bronze -> Silver -> Gold using Snowflake SQL:
- Matches ACTUAL Bronze schema (PARTITION_DATE, EVENT_TIMESTAMP, PRICE)
- No Spark cluster needed
- Runs entirely in Snowflake (pushdown compute)

Schedule: Daily at 1:00 AM UTC
"""

from datetime import datetime, timedelta
from airflow.sdk import dag, task
from pendulum import datetime as pendulum_datetime
import snowflake.connector
import os


default_args = {
    "owner": "FlowGuard",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def get_snowflake_connection():
    """Get Snowflake connection"""
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database="FLOWGUARD_DB",
    )


@dag(
    dag_id="flowguard_daily_etl_sql_v2",
    description="Daily ETL: Bronze -> Silver -> Gold (CORRECTED SCHEMA)",
    start_date=pendulum_datetime(2026, 2, 1, tz="UTC"),
    schedule="0 1 * * *",
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["flowguard", "etl", "sql", "daily", "v2"],
)
def flowguard_daily_etl_sql_v2():
    
    @task
    def bronze_to_silver_orders(ds: str):
        """Transform orders from Bronze to Silver using SQL"""
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        print(f"Processing orders for {ds}...")
        
        # Deduplicate, validate, and insert into Silver
        # Using ACTUAL Bronze columns: PARTITION_DATE, EVENT_TIMESTAMP, PRICE
        sql = f"""
        MERGE INTO SILVER.ORDERS_CLEAN AS target
        USING (
            WITH deduplicated AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY INGESTION_ID ORDER BY EVENT_TIMESTAMP DESC) as rn
                FROM BRONZE.ORDERS_RAW
                WHERE PARTITION_DATE = '{ds}'
            ),
            validated AS (
                SELECT
                    INGESTION_ID,
                    EVENT_ID,
                    ORDER_ID,
                    USER_ID,
                    ITEM_ID,
                    ITEM_NAME,
                    PRICE,
                    STATUS,
                    EVENT_TIMESTAMP,
                    PARTITION_DATE,
                    INGESTION_TIMESTAMP,
                    SOURCE_TOPIC,
                    PRODUCER_SERVICE,
                    
                    -- Validation flags
                    CASE 
                        WHEN ORDER_ID IS NOT NULL 
                        AND USER_ID IS NOT NULL 
                        AND PRICE > 0 
                        AND EVENT_TIMESTAMP IS NOT NULL
                        THEN TRUE ELSE FALSE 
                    END AS IS_VALID,
                    
                    CASE WHEN rn > 1 THEN TRUE ELSE FALSE END AS IS_DUPLICATE,
                    
                    CASE 
                        WHEN ORDER_ID IS NULL THEN 'Missing ORDER_ID'
                        WHEN USER_ID IS NULL THEN 'Missing USER_ID'
                        WHEN PRICE <= 0 THEN 'Invalid PRICE'
                        WHEN EVENT_TIMESTAMP IS NULL THEN 'Missing TIMESTAMP'
                        ELSE NULL
                    END AS VALIDATION_ERRORS
                    
                FROM deduplicated
                WHERE rn = 1
            )
            SELECT * FROM validated
        ) AS source
        ON target.INGESTION_ID = source.INGESTION_ID
        WHEN NOT MATCHED THEN INSERT (
            INGESTION_ID, EVENT_ID, ORDER_ID, USER_ID, ITEM_ID, ITEM_NAME, PRICE,
            STATUS, EVENT_TIMESTAMP, PARTITION_DATE, INGESTION_TIMESTAMP,
            SOURCE_TOPIC, PRODUCER_SERVICE, IS_VALID, IS_DUPLICATE, VALIDATION_ERRORS
        ) VALUES (
            source.INGESTION_ID, source.EVENT_ID, source.ORDER_ID, source.USER_ID,
            source.ITEM_ID, source.ITEM_NAME, source.PRICE, source.STATUS,
            source.EVENT_TIMESTAMP, source.PARTITION_DATE, source.INGESTION_TIMESTAMP,
            source.SOURCE_TOPIC, source.PRODUCER_SERVICE, source.IS_VALID,
            source.IS_DUPLICATE, source.VALIDATION_ERRORS
        );
        """
        
        cursor.execute(sql)
        rows_affected = cursor.rowcount
        
        cursor.close()
        conn.close()
        
        print(f"✅ Processed {rows_affected} orders into SILVER.ORDERS_CLEAN")
        return {"date": ds, "rows": rows_affected}
    
    @task
    def bronze_to_silver_clicks(ds: str):
        """Transform clicks from Bronze to Silver using SQL"""
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        print(f"Processing clicks for {ds}...")
        
        sql = f"""
        MERGE INTO SILVER.CLICKS_CLEAN AS target
        USING (
            WITH deduplicated AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY INGESTION_ID ORDER BY EVENT_TIMESTAMP DESC) as rn
                FROM BRONZE.CLICKS_RAW
                WHERE PARTITION_DATE = '{ds}'
            ),
            validated AS (
                SELECT
                    INGESTION_ID,
                    EVENT_ID,
                    USER_ID,
                    ITEM_ID,
                    EVENT_TYPE,
                    SESSION_ID,
                    EVENT_TIMESTAMP,
                    PARTITION_DATE,
                    INGESTION_TIMESTAMP,
                    SOURCE_TOPIC,
                    PRODUCER_SERVICE,
                    
                    CASE 
                        WHEN EVENT_ID IS NOT NULL 
                        AND USER_ID IS NOT NULL 
                        AND EVENT_TIMESTAMP IS NOT NULL
                        THEN TRUE ELSE FALSE 
                    END AS IS_VALID,
                    
                    CASE WHEN rn > 1 THEN TRUE ELSE FALSE END AS IS_DUPLICATE,
                    
                    CASE 
                        WHEN EVENT_ID IS NULL THEN 'Missing EVENT_ID'
                        WHEN USER_ID IS NULL THEN 'Missing USER_ID'
                        WHEN EVENT_TIMESTAMP IS NULL THEN 'Missing TIMESTAMP'
                        ELSE NULL
                    END AS VALIDATION_ERRORS
                    
                FROM deduplicated
                WHERE rn = 1
            )
            SELECT * FROM validated
        ) AS source
        ON target.INGESTION_ID = source.INGESTION_ID
        WHEN NOT MATCHED THEN INSERT (
            INGESTION_ID, EVENT_ID, USER_ID, ITEM_ID, EVENT_TYPE, SESSION_ID,
            EVENT_TIMESTAMP, PARTITION_DATE, INGESTION_TIMESTAMP, SOURCE_TOPIC,
            PRODUCER_SERVICE, IS_VALID, IS_DUPLICATE, VALIDATION_ERRORS
        ) VALUES (
            source.INGESTION_ID, source.EVENT_ID, source.USER_ID, source.ITEM_ID,
            source.EVENT_TYPE, source.SESSION_ID, source.EVENT_TIMESTAMP,
            source.PARTITION_DATE, source.INGESTION_TIMESTAMP, source.SOURCE_TOPIC,
            source.PRODUCER_SERVICE, source.IS_VALID, source.IS_DUPLICATE, source.VALIDATION_ERRORS
        );
        """
        
        cursor.execute(sql)
        rows_affected = cursor.rowcount
        
        cursor.close()
        conn.close()
        
        print(f"✅ Processed {rows_affected} clicks into SILVER.CLICKS_CLEAN")
        return {"date": ds, "rows": rows_affected}
    
    @task
    def silver_to_gold_gmv(ds: str):
        """Calculate daily GMV metrics"""
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        print(f"Calculating GMV metrics for {ds}...")
        
        sql = f"""
        MERGE INTO GOLD.DAILY_GMV_METRICS AS target
        USING (
            SELECT
                '{ds}'::DATE AS DATE,
                SUM(PRICE) AS TOTAL_REVENUE,
                COUNT(DISTINCT ORDER_ID) AS ORDER_COUNT,
                COUNT(DISTINCT USER_ID) AS UNIQUE_USERS,
                AVG(PRICE) AS AVG_ORDER_VALUE,
                CURRENT_TIMESTAMP() AS CALCULATED_AT
            FROM SILVER.ORDERS_CLEAN
            WHERE PARTITION_DATE = '{ds}'
            AND IS_VALID = TRUE
        ) AS source
        ON target.DATE = source.DATE
        WHEN MATCHED THEN UPDATE SET
            TOTAL_REVENUE = source.TOTAL_REVENUE,
            ORDER_COUNT = source.ORDER_COUNT,
            UNIQUE_USERS = source.UNIQUE_USERS,
            AVG_ORDER_VALUE = source.AVG_ORDER_VALUE,
            CALCULATED_AT = source.CALCULATED_AT
        WHEN NOT MATCHED THEN INSERT (
            DATE, TOTAL_REVENUE, ORDER_COUNT, UNIQUE_USERS, AVG_ORDER_VALUE, CALCULATED_AT
        ) VALUES (
            source.DATE, source.TOTAL_REVENUE, source.ORDER_COUNT,
            source.UNIQUE_USERS, source.AVG_ORDER_VALUE, source.CALCULATED_AT
        );
        """
        
        cursor.execute(sql)
        
        # Get results
        cursor.execute(f"SELECT TOTAL_REVENUE, ORDER_COUNT FROM GOLD.DAILY_GMV_METRICS WHERE DATE = '{ds}'")
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result:
            print(f"✅ GMV calculated: Revenue=${result[0]}, Orders={result[1]}")
            return {"date": ds, "revenue": float(result[0]), "orders": result[1]}
        else:
            print(f"⚠️  No valid orders found for {ds}")
            return {"date": ds, "revenue": 0, "orders": 0}
    
    # Define task dependencies
    orders_task = bronze_to_silver_orders()
    clicks_task = bronze_to_silver_clicks()
    gmv_task = silver_to_gold_gmv()
    
    # Orders must be processed before GMV calculation
    orders_task >> gmv_task
    # Clicks can run in parallel
    clicks_task


# Instantiate the DAG
flowguard_daily_etl_sql_v2()
