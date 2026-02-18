-- =============================================================================
-- FlowGuard Snowflake Schema Setup
-- Run this once to create/update all tables across Bronze, Silver, Gold layers.
-- =============================================================================

USE DATABASE FLOWGUARD_DB;

-- =============================================================================
-- BRONZE LAYER: Raw ingested events (immutable, append-only)
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS BRONZE;

-- Add PARTITION_DATE to existing ORDERS_RAW table (if it already exists)
-- If table doesn't exist yet, CREATE TABLE below handles it.
ALTER TABLE IF EXISTS BRONZE.ORDERS_RAW ADD COLUMN IF NOT EXISTS PARTITION_DATE DATE;
ALTER TABLE IF EXISTS BRONZE.CLICKS_RAW ADD COLUMN IF NOT EXISTS PARTITION_DATE DATE;

-- ORDERS_RAW: Raw order events from Kafka
CREATE TABLE IF NOT EXISTS BRONZE.ORDERS_RAW (
    EVENT_ID         VARCHAR,
    ORDER_ID         VARCHAR,
    USER_ID          VARCHAR,
    ITEM_ID          VARCHAR,
    ITEM_NAME        VARCHAR,
    PRICE            FLOAT,
    STATUS           VARCHAR,
    EVENT_TIMESTAMP  TIMESTAMP_NTZ,
    PARTITION_DATE   DATE,           -- Derived from EVENT_TIMESTAMP for partitioning
    RAW_EVENT        VARIANT,        -- Full JSON payload for debugging
    EVENT_TYPE       VARCHAR,
    SCHEMA_VERSION   VARCHAR,
    SOURCE_TOPIC     VARCHAR,
    PRODUCER_SERVICE VARCHAR,
    INGESTION_ID     VARCHAR         -- Idempotency key: topic-partition-offset
);

-- CLICKS_RAW: Raw click/impression events from Kafka
CREATE TABLE IF NOT EXISTS BRONZE.CLICKS_RAW (
    EVENT_ID         VARCHAR,
    USER_ID          VARCHAR,
    EVENT_TYPE       VARCHAR,        -- 'click' or 'impression'
    ITEM_ID          VARCHAR,
    SESSION_ID       VARCHAR,
    EVENT_TIMESTAMP  TIMESTAMP_NTZ,
    PARTITION_DATE   DATE,           -- Derived from EVENT_TIMESTAMP for partitioning
    RAW_EVENT        VARIANT,        -- Full JSON payload for debugging
    SCHEMA_VERSION   VARCHAR,
    SOURCE_TOPIC     VARCHAR,
    PRODUCER_SERVICE VARCHAR,
    INGESTION_ID     VARCHAR         -- Idempotency key: topic-partition-offset
);

-- =============================================================================
-- SILVER LAYER: Cleaned, validated, deduplicated data
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS SILVER;

-- ORDERS_CLEAN: Validated order events
CREATE TABLE IF NOT EXISTS SILVER.ORDERS_CLEAN (
    ORDER_ID            VARCHAR,
    EVENT_ID            VARCHAR,
    USER_ID             VARCHAR,
    ITEM_ID             VARCHAR,
    ITEM_NAME           VARCHAR,
    PRICE               FLOAT,
    STATUS              VARCHAR,
    ORDER_TIMESTAMP     TIMESTAMP_NTZ,
    DATE_PARTITION      DATE,
    IS_DUPLICATE        BOOLEAN,
    IS_VALID            BOOLEAN,
    VALIDATION_ERRORS   VARCHAR,
    INGESTION_ID        VARCHAR,
    SOURCE_TABLE        VARCHAR,
    LOAD_TIMESTAMP      TIMESTAMP_NTZ,
    PROCESSING_VERSION  VARCHAR
);

-- CLICKS_CLEAN: Validated click/impression events (hover impressions filtered out)
CREATE TABLE IF NOT EXISTS SILVER.CLICKS_CLEAN (
    EVENT_ID            VARCHAR,
    USER_ID             VARCHAR,
    SESSION_ID          VARCHAR,
    ITEM_ID             VARCHAR,
    EVENT_TYPE          VARCHAR,
    CLICK_TIMESTAMP     TIMESTAMP_NTZ,
    DATE_PARTITION      DATE,
    IS_DUPLICATE        BOOLEAN,
    IS_VALID            BOOLEAN,
    VALIDATION_ERRORS   VARCHAR,
    INGESTION_ID        VARCHAR,
    SOURCE_TABLE        VARCHAR,
    LOAD_TIMESTAMP      TIMESTAMP_NTZ,
    PROCESSING_VERSION  VARCHAR
);

-- =============================================================================
-- GOLD LAYER: Aggregated business metrics
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS GOLD;

-- DAILY_GMV_METRICS: Daily revenue and order metrics
CREATE TABLE IF NOT EXISTS GOLD.DAILY_GMV_METRICS (
    DATE            DATE,
    TOTAL_REVENUE   FLOAT,
    ORDER_COUNT     INTEGER,
    UNIQUE_USERS    INTEGER,
    AVG_ORDER_VALUE FLOAT,
    CALCULATED_AT   TIMESTAMP_NTZ
);

-- ADS_ATTRIBUTION: Click-to-order attribution (CTR and conversion)
CREATE TABLE IF NOT EXISTS GOLD.ADS_ATTRIBUTION (
    DATE              DATE,
    ITEM_ID           VARCHAR,
    ITEM_NAME         VARCHAR,
    IMPRESSION_COUNT  INTEGER,
    CLICK_COUNT       INTEGER,
    ORDER_COUNT       INTEGER,
    CTR               FLOAT,         -- click_count / impression_count
    CONVERSION_RATE   FLOAT,         -- order_count / click_count
    TOTAL_REVENUE     FLOAT,
    CALCULATED_AT     TIMESTAMP_NTZ
);

-- =============================================================================
-- Verification queries (run after setup to confirm tables exist)
-- =============================================================================
-- SHOW TABLES IN SCHEMA BRONZE;
-- SHOW TABLES IN SCHEMA SILVER;
-- SHOW TABLES IN SCHEMA GOLD;
-- SELECT COUNT(*) FROM BRONZE.ORDERS_RAW;
-- SELECT COUNT(*) FROM BRONZE.CLICKS_RAW;
-- SELECT EVENT_TIMESTAMP, PARTITION_DATE, ITEM_ID FROM BRONZE.ORDERS_RAW LIMIT 5;
