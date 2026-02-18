"""
FlowGuard Daily ETL Pipeline - SQL-Based (No Spark Required)

This DAG processes Bronze -> Silver -> Gold using Snowflake SQL:
- Much faster for small/medium datasets
- No Spark cluster needed
- Runs entirely in Snowflake (pushdown compute)

Schedule: Daily at 1:00 AM UTC
"""

from datetime import datetime, timedelta
from airflow.sdk import dag, task  # Use SDK instead of decorators
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
    dag_id="flowguard_daily_etl_sql",
    description="Daily ETL: Bronze -> Silver -> Gold (SQL-based)",
    start_date=pendulum_datetime(2026, 2, 1, tz="UTC"),
    schedule="0 1 * * *",
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["flowguard", "etl", "sql", "daily"],
)
def flowguard_daily_etl_sql():
    
    @task
    def bronze_to_silver_orders(ds: str):
        """Transform orders from Bronze to Silver using SQL"""
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        print(f"Processing orders for {ds}...")
        
        # Deduplicate, validate, and insert into Silver
        sql = f"""
        MERGE INTO SILVER.ORDERS_CLEAN AS target
        USING (
            WITH deduplicated AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY INGESTION_ID ORDER BY ORDER_TIMESTAMP DESC) as rn
                FROM BRONZE.ORDERS_RAW
                WHERE DATE_PARTITION = '{ds}'
            ),
            validated AS (
                SELECT
                    INGESTION_ID,
                    EVENT_ID,
                    ORDER_ID,
                    USER_ID,
                    ITEMS,
                    TOTAL_AMOUNT,
                    ORDER_TIMESTAMP,
                    DATE_PARTITION,
                    INGESTED_AT,
                    SOURCE_TOPIC,
                    KAFKA_PARTITION,
                    KAFKA_OFFSET,
                    
                    -- Validation flags
                    CASE 
                        WHEN ORDER_ID IS NOT NULL 
                        AND USER_ID IS NOT NULL 
                        AND TOTAL_AMOUNT > 0 
                        AND ORDER_TIMESTAMP IS NOT NULL
                        THEN TRUE ELSE FALSE 
                    END AS IS_VALID,
                    
                    CASE WHEN rn > 1 THEN TRUE ELSE FALSE END AS IS_DUPLICATE,
                    
                    CASE 
                        WHEN ORDER_ID IS NULL THEN 'Missing ORDER_ID'
                        WHEN USER_ID IS NULL THEN 'Missing USER_ID'
                        WHEN TOTAL_AMOUNT <= 0 THEN 'Invalid TOTAL_AMOUNT'
                        WHEN ORDER_TIMESTAMP IS NULL THEN 'Missing TIMESTAMP'
                        ELSE NULL
                    END AS VALIDATION_ERRORS
                    
                FROM deduplicated
                WHERE rn = 1
            )
            SELECT * FROM validated
        ) AS source
        ON target.INGESTION_ID = source.INGESTION_ID
        WHEN NOT MATCHED THEN INSERT (
            INGESTION_ID, EVENT_ID, ORDER_ID, USER_ID, ITEMS, TOTAL_AMOUNT,
            ORDER_TIMESTAMP, DATE_PARTITION, INGESTED_AT, SOURCE_TOPIC,
            KAFKA_PARTITION, KAFKA_OFFSET, IS_VALID, IS_DUPLICATE, VALIDATION_ERRORS
        ) VALUES (
            source.INGESTION_ID, source.EVENT_ID, source.ORDER_ID, source.USER_ID,
            source.ITEMS, source.TOTAL_AMOUNT, source.ORDER_TIMESTAMP,
            source.DATE_PARTITION, source.INGESTED_AT, source.SOURCE_TOPIC,
            source.KAFKA_PARTITION, source.KAFKA_OFFSET, source.IS_VALID,
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
                    ROW_NUMBER() OVER (PARTITION BY INGESTION_ID ORDER BY CLICK_TIMESTAMP DESC) as rn
                FROM BRONZE.CLICKS_RAW
                WHERE DATE_PARTITION = '{ds}'
            ),
            validated AS (
                SELECT
                    INGESTION_ID,
                    EVENT_ID,
                    USER_ID,
                    FOOD_ITEM_ID,
                    EVENT_TYPE,
                    CLICK_TIMESTAMP,
                    DATE_PARTITION,
                    INGESTED_AT,
                    SOURCE_TOPIC,
                    KAFKA_PARTITION,
                    KAFKA_OFFSET,
                    
                    CASE 
                        WHEN EVENT_ID IS NOT NULL 
                        AND USER_ID IS NOT NULL 
                        AND CLICK_TIMESTAMP IS NOT NULL
                        THEN TRUE ELSE FALSE 
                    END AS IS_VALID,
                    
                    CASE WHEN rn > 1 THEN TRUE ELSE FALSE END AS IS_DUPLICATE,
                    
                    CASE 
                        WHEN EVENT_ID IS NULL THEN 'Missing EVENT_ID'
                        WHEN USER_ID IS NULL THEN 'Missing USER_ID'
                        WHEN CLICK_TIMESTAMP IS NULL THEN 'Missing TIMESTAMP'
                        ELSE NULL
                    END AS VALIDATION_ERRORS
                    
                FROM deduplicated
                WHERE rn = 1
            )
            SELECT * FROM validated
        ) AS source
        ON target.INGESTION_ID = source.INGESTION_ID
        WHEN NOT MATCHED THEN INSERT (
            INGESTION_ID, EVENT_ID, USER_ID, FOOD_ITEM_ID, EVENT_TYPE,
            CLICK_TIMESTAMP, DATE_PARTITION, INGESTED_AT, SOURCE_TOPIC,
            KAFKA_PARTITION, KAFKA_OFFSET, IS_VALID, IS_DUPLICATE, VALIDATION_ERRORS
        ) VALUES (
            source.INGESTION_ID, source.EVENT_ID, source.USER_ID, source.FOOD_ITEM_ID,
            source.EVENT_TYPE, source.CLICK_TIMESTAMP, source.DATE_PARTITION,
            source.INGESTED_AT, source.SOURCE_TOPIC, source.KAFKA_PARTITION,
            source.KAFKA_OFFSET, source.IS_VALID, source.IS_DUPLICATE, source.VALIDATION_ERRORS
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
                SUM(TOTAL_AMOUNT) AS TOTAL_REVENUE,
                COUNT(DISTINCT ORDER_ID) AS ORDER_COUNT,
                COUNT(DISTINCT USER_ID) AS UNIQUE_USERS,
                AVG(TOTAL_AMOUNT) AS AVG_ORDER_VALUE,
                CURRENT_TIMESTAMP() AS CALCULATED_AT
            FROM SILVER.ORDERS_CLEAN
            WHERE DATE_PARTITION = '{ds}'
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
            print(f"✅ GMV: ${result[0]:,.2f}, Orders: {result[1]}")
            return {"date": ds, "revenue": float(result[0]), "orders": result[1]}
        
        return {"date": ds, "revenue": 0, "orders": 0}
    
    @task
    def validate_pipeline(ds: str, **context):
        """Validate pipeline results"""
        ti = context['ti']
        
        # Get results from upstream tasks
        silver_orders = ti.xcom_pull(task_ids='bronze_to_silver_orders')
        silver_clicks = ti.xcom_pull(task_ids='bronze_to_silver_clicks')
        gold_gmv = ti.xcom_pull(task_ids='silver_to_gold_gmv')
        
        print(f"\n{'='*50}")
        print(f"ETL Pipeline Results for {ds}")
        print(f"{'='*50}")
        print(f"Silver Orders: {silver_orders.get('rows', 0)} rows")
        print(f"Silver Clicks: {silver_clicks.get('rows', 0)} rows")
        print(f"Gold Revenue: ${gold_gmv.get('revenue', 0):,.2f}")
        print(f"Gold Orders: {gold_gmv.get('orders', 0)}")
        print(f"{'='*50}\n")
        
        return True
    
    # Define task flow
    orders_task = bronze_to_silver_orders("{{ ds }}")
    clicks_task = bronze_to_silver_clicks("{{ ds }}")
    gmv_task = silver_to_gold_gmv("{{ ds }}")
    validate_task = validate_pipeline("{{ ds }}")
    
    # Dependencies
    [orders_task, clicks_task] >> gmv_task >> validate_task


# Instantiate DAG
dag_instance = flowguard_daily_etl_sql()
