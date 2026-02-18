"""
Spark Job: Bronze → Silver (Orders)

Reads BRONZE.ORDERS_RAW for a given date, deduplicates, validates,
and writes to SILVER.ORDERS_CLEAN.

Usage:
    spark-submit spark/jobs/bronze_to_silver_orders.py --date 2026-02-18
"""

import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_spark_session() -> SparkSession:
    """Create Spark session with Snowflake connector."""
    return (
        SparkSession.builder
        .appName("FlowGuard-BronzeToSilverOrders")
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,net.snowflake:snowflake-jdbc:3.14.4")
        .getOrCreate()
    )


def get_snowflake_options(schema: str) -> dict:
    """Build Snowflake connection options from environment variables."""
    import os
    return {
        "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
        "sfUser": os.environ["SNOWFLAKE_USER"],
        "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
        "sfDatabase": os.environ.get("SNOWFLAKE_DATABASE", "FLOWGUARD_DB"),
        "sfWarehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "sfSchema": schema,
    }


def run(date_str: str, spark: SparkSession):
    """
    Main ETL logic: Bronze.ORDERS_RAW → Silver.ORDERS_CLEAN for a given date.
    
    Steps:
    1. Read Bronze orders for the given partition date
    2. Deduplicate by INGESTION_ID (idempotency key)
    3. Validate required fields (ORDER_ID, USER_ID, PRICE > 0, TIMESTAMP)
    4. Write to Silver with quality flags
    """
    logger.info(f"=== Bronze → Silver (Orders) for date: {date_str} ===")

    bronze_opts = get_snowflake_options("BRONZE")
    silver_opts = get_snowflake_options("SILVER")

    # ── Step 1: Read Bronze ──────────────────────────────────────────────────
    logger.info("Reading BRONZE.ORDERS_RAW...")
    bronze_df = (
        spark.read
        .format("net.snowflake.spark.snowflake")
        .options(**bronze_opts)
        .option("query", f"""
            SELECT *
            FROM BRONZE.ORDERS_RAW
            WHERE PARTITION_DATE = '{date_str}'
        """)
        .load()
    )

    raw_count = bronze_df.count()
    logger.info(f"  Read {raw_count} raw order events for {date_str}")

    if raw_count == 0:
        logger.warning(f"  No orders found for {date_str}. Skipping.")
        return

    # ── Step 2: Deduplicate by INGESTION_ID ─────────────────────────────────
    logger.info("Deduplicating by INGESTION_ID...")
    window = Window.partitionBy("INGESTION_ID").orderBy(F.col("EVENT_TIMESTAMP").desc())
    deduped_df = (
        bronze_df
        .withColumn("_rn", F.row_number().over(window))
        .withColumn("IS_DUPLICATE", F.col("_rn") > 1)
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── Step 3: Validate fields ──────────────────────────────────────────────
    logger.info("Validating fields...")
    validated_df = (
        deduped_df
        .withColumn(
            "IS_VALID",
            F.col("ORDER_ID").isNotNull() &
            F.col("USER_ID").isNotNull() &
            (F.col("PRICE") > 0) &
            F.col("EVENT_TIMESTAMP").isNotNull()
        )
        .withColumn(
            "VALIDATION_ERRORS",
            F.when(F.col("ORDER_ID").isNull(), "Missing ORDER_ID")
             .when(F.col("USER_ID").isNull(), "Missing USER_ID")
             .when(F.col("PRICE") <= 0, "Invalid PRICE")
             .when(F.col("EVENT_TIMESTAMP").isNull(), "Missing TIMESTAMP")
             .otherwise(None)
        )
    )

    # ── Step 4: Select Silver schema columns ─────────────────────────────────
    silver_df = validated_df.select(
        F.col("ORDER_ID"),
        F.col("EVENT_ID"),
        F.col("USER_ID"),
        F.col("ITEM_ID"),
        F.col("ITEM_NAME"),
        F.col("PRICE"),
        F.col("STATUS"),
        F.col("EVENT_TIMESTAMP").alias("ORDER_TIMESTAMP"),
        F.col("PARTITION_DATE").alias("DATE_PARTITION"),
        F.col("IS_DUPLICATE"),
        F.col("IS_VALID"),
        F.col("VALIDATION_ERRORS"),
        F.col("INGESTION_ID"),
        F.lit("BRONZE.ORDERS_RAW").alias("SOURCE_TABLE"),
        F.lit(datetime.utcnow()).alias("LOAD_TIMESTAMP"),
        F.lit("v1.0").alias("PROCESSING_VERSION"),
    )

    valid_count = silver_df.filter(F.col("IS_VALID")).count()
    invalid_count = silver_df.filter(~F.col("IS_VALID")).count()
    logger.info(f"  Valid: {valid_count}, Invalid: {invalid_count}")

    # ── Step 5: Write to Silver ──────────────────────────────────────────────
    logger.info("Writing to SILVER.ORDERS_CLEAN...")
    (
        silver_df.write
        .format("net.snowflake.spark.snowflake")
        .options(**silver_opts)
        .option("dbtable", "ORDERS_CLEAN")
        .mode("append")
        .save()
    )

    logger.info(f"✅ Done: {valid_count} valid orders written to SILVER.ORDERS_CLEAN for {date_str}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze → Silver ETL for Orders")
    parser.add_argument("--date", required=True, help="Partition date (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = get_spark_session()
    try:
        run(args.date, spark)
    finally:
        spark.stop()
