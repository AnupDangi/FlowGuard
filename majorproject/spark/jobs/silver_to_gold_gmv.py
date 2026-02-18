"""
Spark Job: Silver → Gold (Daily GMV Metrics)

Reads SILVER.ORDERS_CLEAN for a given date and aggregates daily
revenue, order count, unique users, and avg order value into GOLD.DAILY_GMV_METRICS.

Usage:
    spark-submit spark/jobs/silver_to_gold_gmv.py --date 2026-02-18
"""

import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FlowGuard-SilverToGoldGMV")
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
    Aggregate SILVER.ORDERS_CLEAN → GOLD.DAILY_GMV_METRICS for a given date.
    Only valid (IS_VALID=True) orders are included.
    """
    logger.info(f"=== Silver → Gold (GMV) for date: {date_str} ===")

    silver_opts = get_snowflake_options("SILVER")
    gold_opts = get_snowflake_options("GOLD")

    # ── Step 1: Read Silver orders ───────────────────────────────────────────
    logger.info("Reading SILVER.ORDERS_CLEAN...")
    silver_df = (
        spark.read
        .format("net.snowflake.spark.snowflake")
        .options(**silver_opts)
        .option("query", f"""
            SELECT ORDER_ID, USER_ID, PRICE, DATE_PARTITION
            FROM SILVER.ORDERS_CLEAN
            WHERE DATE_PARTITION = '{date_str}'
              AND IS_VALID = TRUE
        """)
        .load()
    )

    order_count = silver_df.count()
    logger.info(f"  Read {order_count} valid orders for {date_str}")

    if order_count == 0:
        logger.warning(f"  No valid orders for {date_str}. Writing zero-row metric.")

    # ── Step 2: Aggregate GMV metrics ────────────────────────────────────────
    logger.info("Aggregating GMV metrics...")
    gmv_df = silver_df.agg(
        F.sum("PRICE").alias("TOTAL_REVENUE"),
        F.count("ORDER_ID").alias("ORDER_COUNT"),
        F.countDistinct("USER_ID").alias("UNIQUE_USERS"),
        F.avg("PRICE").alias("AVG_ORDER_VALUE"),
    ).withColumn("DATE", F.lit(date_str).cast("date")) \
     .withColumn("CALCULATED_AT", F.lit(datetime.utcnow()))

    # Reorder columns to match Gold table schema
    gmv_df = gmv_df.select(
        "DATE", "TOTAL_REVENUE", "ORDER_COUNT", "UNIQUE_USERS", "AVG_ORDER_VALUE", "CALCULATED_AT"
    )

    # Log the result
    result = gmv_df.collect()[0]
    logger.info(
        f"  GMV: Revenue=₹{result['TOTAL_REVENUE'] or 0:.2f}, "
        f"Orders={result['ORDER_COUNT']}, "
        f"Users={result['UNIQUE_USERS']}, "
        f"AOV=₹{result['AVG_ORDER_VALUE'] or 0:.2f}"
    )

    # ── Step 3: Write to Gold (overwrite for this date) ──────────────────────
    # Delete existing row for this date first (idempotent re-run)
    logger.info("Writing to GOLD.DAILY_GMV_METRICS...")
    (
        gmv_df.write
        .format("net.snowflake.spark.snowflake")
        .options(**gold_opts)
        .option("dbtable", "DAILY_GMV_METRICS")
        .option("preactions", f"DELETE FROM GOLD.DAILY_GMV_METRICS WHERE DATE = '{date_str}'")
        .mode("append")
        .save()
    )

    logger.info(f"✅ Done: GMV metrics written to GOLD.DAILY_GMV_METRICS for {date_str}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver → Gold ETL for GMV Metrics")
    parser.add_argument("--date", required=True, help="Partition date (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = get_spark_session()
    try:
        run(args.date, spark)
    finally:
        spark.stop()
