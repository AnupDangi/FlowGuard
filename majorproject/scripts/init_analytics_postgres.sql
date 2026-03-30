-- FlowGuard Analytics PostgreSQL Initialization
-- Creates Bronze/Silver/Gold schemas and core tables for local analytics.

CREATE SCHEMA IF NOT EXISTS analytics;

-- ==================== Bronze ====================
CREATE TABLE IF NOT EXISTS analytics.bronze_orders_raw (
    event_id TEXT,
    order_id TEXT,
    user_id BIGINT,
    item_id BIGINT,
    item_name TEXT,
    price NUMERIC(12, 2),
    status TEXT,
    event_timestamp TIMESTAMPTZ,
    raw_event JSONB NOT NULL,
    event_type TEXT,
    schema_version TEXT,
    source_topic TEXT,
    producer_service TEXT,
    ingestion_id TEXT PRIMARY KEY,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    ingestion_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    partition_date DATE GENERATED ALWAYS AS ((event_timestamp AT TIME ZONE 'UTC')::DATE) STORED
);

CREATE INDEX IF NOT EXISTS idx_bronze_orders_partition_date ON analytics.bronze_orders_raw(partition_date);
CREATE INDEX IF NOT EXISTS idx_bronze_orders_event_timestamp ON analytics.bronze_orders_raw(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_bronze_orders_item_user ON analytics.bronze_orders_raw(item_id, user_id);

CREATE TABLE IF NOT EXISTS analytics.bronze_clicks_raw (
    event_id TEXT,
    user_id BIGINT,
    event_type TEXT,
    item_id BIGINT,
    session_id TEXT,
    event_timestamp TIMESTAMPTZ,
    raw_event JSONB NOT NULL,
    schema_version TEXT,
    source_topic TEXT,
    producer_service TEXT,
    ingestion_id TEXT PRIMARY KEY,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    ingestion_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    partition_date DATE GENERATED ALWAYS AS ((event_timestamp AT TIME ZONE 'UTC')::DATE) STORED
);

CREATE INDEX IF NOT EXISTS idx_bronze_clicks_partition_date ON analytics.bronze_clicks_raw(partition_date);
CREATE INDEX IF NOT EXISTS idx_bronze_clicks_event_timestamp ON analytics.bronze_clicks_raw(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_bronze_clicks_item_user ON analytics.bronze_clicks_raw(item_id, user_id);

-- ==================== Silver ====================
CREATE TABLE IF NOT EXISTS analytics.silver_orders_clean (
    order_id TEXT,
    event_id TEXT,
    user_id TEXT,
    item_id TEXT,
    item_name TEXT,
    price NUMERIC(12, 2),
    status TEXT,
    order_timestamp TIMESTAMPTZ,
    date_partition DATE,
    enriched_category TEXT,
    enriched_description TEXT,
    enriched_preparation_time INTEGER,
    is_duplicate BOOLEAN NOT NULL DEFAULT FALSE,
    is_valid BOOLEAN NOT NULL DEFAULT TRUE,
    validation_errors TEXT,
    ingestion_id TEXT,
    source_table TEXT,
    load_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processing_version TEXT NOT NULL DEFAULT 'v1.0'
);

CREATE INDEX IF NOT EXISTS idx_silver_orders_date_partition ON analytics.silver_orders_clean(date_partition);
CREATE INDEX IF NOT EXISTS idx_silver_orders_item ON analytics.silver_orders_clean(item_id);
CREATE INDEX IF NOT EXISTS idx_silver_orders_user ON analytics.silver_orders_clean(user_id);

CREATE TABLE IF NOT EXISTS analytics.silver_clicks_clean (
    event_id TEXT,
    user_id TEXT,
    session_id TEXT,
    item_id TEXT,
    event_type TEXT,
    click_timestamp TIMESTAMPTZ,
    date_partition DATE,
    enriched_item_name TEXT,
    enriched_category TEXT,
    is_duplicate BOOLEAN NOT NULL DEFAULT FALSE,
    is_valid BOOLEAN NOT NULL DEFAULT TRUE,
    validation_errors TEXT,
    ingestion_id TEXT,
    source_table TEXT,
    load_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processing_version TEXT NOT NULL DEFAULT 'v1.0'
);

CREATE INDEX IF NOT EXISTS idx_silver_clicks_date_partition ON analytics.silver_clicks_clean(date_partition);
CREATE INDEX IF NOT EXISTS idx_silver_clicks_item ON analytics.silver_clicks_clean(item_id);
CREATE INDEX IF NOT EXISTS idx_silver_clicks_user ON analytics.silver_clicks_clean(user_id);

-- ==================== Gold ====================
CREATE TABLE IF NOT EXISTS analytics.gold_daily_gmv_metrics (
    date DATE PRIMARY KEY,
    total_revenue NUMERIC(14, 2) NOT NULL DEFAULT 0,
    avg_order_value NUMERIC(14, 2) NOT NULL DEFAULT 0,
    order_count BIGINT NOT NULL DEFAULT 0,
    total_items_sold BIGINT NOT NULL DEFAULT 0,
    unique_users BIGINT NOT NULL DEFAULT 0,
    new_users BIGINT NOT NULL DEFAULT 0,
    returning_users BIGINT NOT NULL DEFAULT 0,
    revenue_growth_pct NUMERIC(8, 4) NOT NULL DEFAULT 0,
    order_growth_pct NUMERIC(8, 4) NOT NULL DEFAULT 0,
    load_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processing_version TEXT NOT NULL DEFAULT 'v1.0'
);

CREATE TABLE IF NOT EXISTS analytics.gold_ads_attribution (
    date DATE NOT NULL,
    item_id TEXT NOT NULL,
    item_name TEXT,
    impression_count BIGINT NOT NULL DEFAULT 0,
    click_count BIGINT NOT NULL DEFAULT 0,
    order_count BIGINT NOT NULL DEFAULT 0,
    ctr NUMERIC(10, 6) NOT NULL DEFAULT 0,
    conversion_rate NUMERIC(10, 6) NOT NULL DEFAULT 0,
    total_revenue NUMERIC(14, 2) NOT NULL DEFAULT 0,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date, item_id)
);

-- Reconciliation table for periodic overwrite-if-greater totals.
CREATE TABLE IF NOT EXISTS analytics.recon_hourly_totals (
    date_hour TIMESTAMPTZ NOT NULL,
    item_id TEXT NOT NULL,
    impressions BIGINT NOT NULL DEFAULT 0,
    clicks BIGINT NOT NULL DEFAULT 0,
    orders BIGINT NOT NULL DEFAULT 0,
    attributed_orders BIGINT NOT NULL DEFAULT 0,
    revenue NUMERIC(14, 2) NOT NULL DEFAULT 0,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date_hour, item_id)
);
