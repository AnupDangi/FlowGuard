"""
Spark Job: Bronze → Silver (Clicks)

Reads BRONZE.CLICKS_RAW for a given date, deduplicates, validates,
filters out hover-based noise (legacy impressions from onMouseEnter),
and writes to SILVER.CLICKS_CLEAN.

Usage:
    spark-submit spark/jobs/bronze_to_silver_clicks.py --date 2026-02-18
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
    return (
        SparkSession.builder
        .appName("FlowGuard-BronzeToSilverClicks")
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,net.snowflake:snowflake-jdbc:3.14.4")
        .getOrCreate()
    )


def get_snowflake_options(schema: str) -> dict:
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
    Main ETL logic: Bronze.CLICKS_RAW → Silver.CLICKS_CLEAN for a given date.

    Steps:
    1. Read Bronze clicks for the given partition date
    2. Deduplicate by INGESTION_ID
    3. Validate required fields
    4. Write to Silver with quality flags
    
    Note: Old hover-based 'impression' events (from onMouseEnter, pre-fix) are
    kept in Bronze for audit but are flagged IS_VALID=False with VALIDATION_ERRORS
    = 'hover_impression_noise' so they don't pollute Gold metrics.
    """
    logger.info(f"=== Bronze → Silver (Clicks) for date: {date_str} ===")

    bronze_opts = get_snowflake_options("BRONZE")
    silver_opts = get_snowflake_options("SILVER")

    # ── Step 1: Read Bronze ──────────────────────────────────────────────────
    logger.info("Reading BRONZE.CLICKS_RAW...")
    bronze_df = (
        spark.read
        .format("net.snowflake.spark.snowflake")
        .options(**bronze_opts)
        .option("query", f"""
            SELECT *
            FROM BRONZE.CLICKS_RAW
            WHERE PARTITION_DATE = '{date_str}'
        """)
        .load()
    )

    raw_count = bronze_df.count()
    logger.info(f"  Read {raw_count} raw click events for {date_str}")

    if raw_count == 0:
        logger.warning(f"  No clicks found for {date_str}. Skipping.")
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
    # 'impression' events from the fixed web app (detail page load) are valid.
    # Any impression that has no ITEM_ID is likely hover noise from before the fix.
    logger.info("Validating fields...")
    validated_df = (
        deduped_df
        .withColumn(
            "IS_VALID",
            F.col("EVENT_ID").isNotNull() &
            F.col("USER_ID").isNotNull() &
            F.col("EVENT_TIMESTAMP").isNotNull() &
            F.col("ITEM_ID").isNotNull()  # Hover noise events often have null item_id
        )
        .withColumn(
            "VALIDATION_ERRORS",
            F.when(F.col("EVENT_ID").isNull(), "Missing EVENT_ID")
             .when(F.col("USER_ID").isNull(), "Missing USER_ID")
             .when(F.col("EVENT_TIMESTAMP").isNull(), "Missing TIMESTAMP")
             .when(F.col("ITEM_ID").isNull(), "hover_impression_noise")
             .otherwise(None)
        )
    )

    # ── Step 4: Select Silver schema columns ─────────────────────────────────
    silver_df = validated_df.select(
        F.col("EVENT_ID"),
        F.col("USER_ID"),
        F.col("SESSION_ID"),
        F.col("ITEM_ID"),
        F.col("EVENT_TYPE"),
        F.col("EVENT_TIMESTAMP").alias("CLICK_TIMESTAMP"),
        F.col("PARTITION_DATE").alias("DATE_PARTITION"),
        F.col("IS_DUPLICATE"),
        F.col("IS_VALID"),
        F.col("VALIDATION_ERRORS"),
        F.col("INGESTION_ID"),
        F.lit("BRONZE.CLICKS_RAW").alias("SOURCE_TABLE"),
        F.lit(datetime.utcnow()).alias("LOAD_TIMESTAMP"),
        F.lit("v1.0").alias("PROCESSING_VERSION"),
    )

    valid_count = silver_df.filter(F.col("IS_VALID")).count()
    invalid_count = silver_df.filter(~F.col("IS_VALID")).count()
    logger.info(f"  Valid: {valid_count}, Invalid (noise/missing): {invalid_count}")

    # ── Step 5: Write to Silver ──────────────────────────────────────────────
    logger.info("Writing to SILVER.CLICKS_CLEAN...")
    (
        silver_df.write
        .format("net.snowflake.spark.snowflake")
        .options(**silver_opts)
        .option("dbtable", "CLICKS_CLEAN")
        .mode("append")
        .save()
    )

    logger.info(f"✅ Done: {valid_count} valid clicks written to SILVER.CLICKS_CLEAN for {date_str}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze → Silver ETL for Clicks")
    parser.add_argument("--date", required=True, help="Partition date (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = get_spark_session()
    try:
        run(args.date, spark)
    finally:
        spark.stop()
