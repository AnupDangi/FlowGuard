"""
Spark Job: Silver → Gold (Ads Attribution)

Joins SILVER.CLICKS_CLEAN + SILVER.ORDERS_CLEAN for a given date to compute
per-item CTR and conversion rate — the core ads attribution metric.

Attribution window: A click is attributed to an order if the order happens
within 30 minutes of the click, for the same user and item.

Usage:
    spark-submit spark/jobs/silver_to_gold_ads.py --date 2026-02-18
"""

import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ATTRIBUTION_WINDOW_SECONDS = 30 * 60  # 30 minutes


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FlowGuard-SilverToGoldAds")
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
    Compute ads attribution metrics for a given date.
    
    For each food item:
    - impression_count: how many times users navigated to the detail page
    - click_count: how many times users clicked "Order Now"
    - order_count: how many orders were placed (attributed via 30-min window)
    - CTR = click_count / impression_count
    - conversion_rate = order_count / click_count
    """
    logger.info(f"=== Silver → Gold (Ads Attribution) for date: {date_str} ===")

    silver_opts = get_snowflake_options("SILVER")
    gold_opts = get_snowflake_options("GOLD")

    # ── Step 1: Read Silver clicks (valid only) ──────────────────────────────
    logger.info("Reading SILVER.CLICKS_CLEAN...")
    clicks_df = (
        spark.read
        .format("net.snowflake.spark.snowflake")
        .options(**silver_opts)
        .option("query", f"""
            SELECT EVENT_ID, USER_ID, ITEM_ID, EVENT_TYPE, CLICK_TIMESTAMP
            FROM SILVER.CLICKS_CLEAN
            WHERE DATE_PARTITION = '{date_str}'
              AND IS_VALID = TRUE
        """)
        .load()
    )

    # ── Step 2: Read Silver orders (valid only) ──────────────────────────────
    logger.info("Reading SILVER.ORDERS_CLEAN...")
    orders_df = (
        spark.read
        .format("net.snowflake.spark.snowflake")
        .options(**silver_opts)
        .option("query", f"""
            SELECT ORDER_ID, USER_ID, ITEM_ID, ITEM_NAME, PRICE, ORDER_TIMESTAMP
            FROM SILVER.ORDERS_CLEAN
            WHERE DATE_PARTITION = '{date_str}'
              AND IS_VALID = TRUE
        """)
        .load()
    )

    click_count_total = clicks_df.count()
    order_count_total = orders_df.count()
    logger.info(f"  Clicks: {click_count_total}, Orders: {order_count_total}")

    # ── Step 3: Per-item impression and click counts ─────────────────────────
    impressions_df = (
        clicks_df.filter(F.col("EVENT_TYPE") == "impression")
        .groupBy("ITEM_ID")
        .agg(F.count("EVENT_ID").alias("IMPRESSION_COUNT"))
    )

    clicks_agg_df = (
        clicks_df.filter(F.col("EVENT_TYPE") == "click")
        .groupBy("ITEM_ID")
        .agg(F.count("EVENT_ID").alias("CLICK_COUNT"))
    )

    # ── Step 4: Attribution — join clicks to orders within 30-min window ─────
    # For each click, find orders by the same user for the same item
    # within ATTRIBUTION_WINDOW_SECONDS after the click.
    logger.info("Computing attribution (30-min window)...")
    attributed_df = (
        clicks_df.alias("c")
        .join(orders_df.alias("o"), on=[
            F.col("c.USER_ID") == F.col("o.USER_ID"),
            F.col("c.ITEM_ID") == F.col("o.ITEM_ID"),
            F.col("o.ORDER_TIMESTAMP").cast("long") >= F.col("c.CLICK_TIMESTAMP").cast("long"),
            F.col("o.ORDER_TIMESTAMP").cast("long") <= (
                F.col("c.CLICK_TIMESTAMP").cast("long") + ATTRIBUTION_WINDOW_SECONDS
            ),
        ], how="inner")
        .groupBy(F.col("c.ITEM_ID"))
        .agg(
            F.countDistinct("o.ORDER_ID").alias("ATTRIBUTED_ORDER_COUNT"),
            F.sum("o.PRICE").alias("ATTRIBUTED_REVENUE"),
        )
    )

    # ── Step 5: Per-item order totals (for item names) ───────────────────────
    orders_per_item_df = (
        orders_df.groupBy("ITEM_ID", "ITEM_NAME")
        .agg(
            F.count("ORDER_ID").alias("ORDER_COUNT"),
            F.sum("PRICE").alias("TOTAL_REVENUE"),
        )
    )

    # ── Step 6: Combine all metrics ──────────────────────────────────────────
    all_items_df = (
        orders_per_item_df
        .join(impressions_df, on="ITEM_ID", how="left")
        .join(clicks_agg_df, on="ITEM_ID", how="left")
        .join(attributed_df, on="ITEM_ID", how="left")
        .fillna(0, subset=["IMPRESSION_COUNT", "CLICK_COUNT", "ATTRIBUTED_ORDER_COUNT", "ATTRIBUTED_REVENUE"])
        .withColumn("DATE", F.lit(date_str).cast("date"))
        .withColumn(
            "CTR",
            F.when(F.col("IMPRESSION_COUNT") > 0,
                   F.col("CLICK_COUNT") / F.col("IMPRESSION_COUNT"))
             .otherwise(0.0)
        )
        .withColumn(
            "CONVERSION_RATE",
            F.when(F.col("CLICK_COUNT") > 0,
                   F.col("ATTRIBUTED_ORDER_COUNT") / F.col("CLICK_COUNT"))
             .otherwise(0.0)
        )
        .withColumn("CALCULATED_AT", F.lit(datetime.utcnow()))
        .select(
            "DATE", "ITEM_ID", "ITEM_NAME",
            "IMPRESSION_COUNT", "CLICK_COUNT",
            F.col("ORDER_COUNT"),
            "CTR", "CONVERSION_RATE",
            "TOTAL_REVENUE",
            "CALCULATED_AT",
        )
    )

    logger.info(f"  Attribution computed for {all_items_df.count()} items")

    # ── Step 7: Write to Gold ────────────────────────────────────────────────
    logger.info("Writing to GOLD.ADS_ATTRIBUTION...")
    (
        all_items_df.write
        .format("net.snowflake.spark.snowflake")
        .options(**gold_opts)
        .option("dbtable", "ADS_ATTRIBUTION")
        .option("preactions", f"DELETE FROM GOLD.ADS_ATTRIBUTION WHERE DATE = '{date_str}'")
        .mode("append")
        .save()
    )

    logger.info(f"✅ Done: Ads attribution written to GOLD.ADS_ATTRIBUTION for {date_str}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver → Gold ETL for Ads Attribution")
    parser.add_argument("--date", required=True, help="Partition date (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = get_spark_session()
    try:
        run(args.date, spark)
    finally:
        spark.stop()
