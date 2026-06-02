-- ClickHouse initialization: warehouse database, tables, materialized views.
-- Runs on first container start via docker-entrypoint-initdb.d.

CREATE DATABASE IF NOT EXISTS etl_warehouse;

-- ── Dimension Tables ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS etl_warehouse.dim_date (
    date_key    UInt32,
    full_date   Date,
    year        UInt16,
    quarter     UInt8,
    month       UInt8,
    week        UInt8,
    day_of_week UInt8,
    is_weekend  UInt8,
    PRIMARY KEY (date_key)
) ENGINE = MergeTree()
ORDER BY date_key;

CREATE TABLE IF NOT EXISTS etl_warehouse.dim_customer (
    customer_id  String,
    email        String,
    name         String,
    region       String,
    created_at   DateTime,
    _loaded_at   DateTime DEFAULT now(),
    PRIMARY KEY (customer_id)
) ENGINE = ReplacingMergeTree(_loaded_at)
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS etl_warehouse.dim_product (
    product_id  String,
    name        String,
    category    String,
    price       Decimal(10, 2),
    _loaded_at  DateTime DEFAULT now(),
    PRIMARY KEY (product_id)
) ENGINE = ReplacingMergeTree(_loaded_at)
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS etl_warehouse.dim_sensor_location (
    location_id   String,
    site_name     String,
    latitude      Float64,
    longitude     Float64,
    sensor_type   String,
    _loaded_at    DateTime DEFAULT now(),
    PRIMARY KEY (location_id)
) ENGINE = ReplacingMergeTree(_loaded_at)
ORDER BY location_id;

-- ── Fact Tables ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS etl_warehouse.fact_sales (
    sale_id      String,
    order_id     String,
    customer_id  String,
    product_id   String,
    date_key     UInt32,
    quantity     UInt32,
    unit_price   Decimal(10, 2),
    total_amount Decimal(12, 2),
    status       String,
    event_ts     DateTime,
    _batch_id    String,
    _loaded_at   DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_ts)
ORDER BY (date_key, customer_id, product_id)
TTL event_ts + INTERVAL 2 YEAR;

CREATE TABLE IF NOT EXISTS etl_warehouse.fact_sensor_readings (
    reading_id   String,
    location_id  String,
    date_key     UInt32,
    temperature  Float32,
    pressure     Float32,
    humidity     Float32,
    lat          Float64,
    lon          Float64,
    event_ts     DateTime,
    _loaded_at   DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (location_id, event_ts)
TTL event_ts + INTERVAL 1 YEAR;

CREATE TABLE IF NOT EXISTS etl_warehouse.fact_trades (
    trade_id     String,
    asset        String,
    side         String,
    quantity     Float64,
    price        Decimal(18, 8),
    total_value  Decimal(18, 4),
    date_key     UInt32,
    event_ts     DateTime,
    _loaded_at   DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_ts)
ORDER BY (date_key, asset)
TTL event_ts + INTERVAL 3 YEAR;

CREATE TABLE IF NOT EXISTS etl_warehouse.fact_social_engagement (
    post_id      String,
    user_id      String,
    platform     String,
    content_type String,
    likes        UInt32,
    shares       UInt32,
    comments     UInt32,
    date_key     UInt32,
    event_ts     DateTime,
    _loaded_at   DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_ts)
ORDER BY (date_key, platform);

-- ── Aggregation Tables (pre-computed for dashboards) ──────────────

CREATE TABLE IF NOT EXISTS etl_warehouse.agg_daily_revenue (
    date_key     UInt32,
    full_date    Date,
    region       String,
    category     String,
    total_orders UInt32,
    total_revenue Decimal(18, 2),
    avg_order_value Decimal(10, 2),
    _computed_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(full_date)
ORDER BY (full_date, region, category);

CREATE TABLE IF NOT EXISTS etl_warehouse.agg_hourly_sensor (
    hour_ts      DateTime,
    location_id  String,
    avg_temp     Float32,
    avg_pressure Float32,
    avg_humidity Float32,
    reading_count UInt32,
    anomaly_flag UInt8,
    _computed_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(hour_ts)
ORDER BY (hour_ts, location_id);

CREATE TABLE IF NOT EXISTS etl_warehouse.agg_trading_volume (
    date_key     UInt32,
    full_date    Date,
    asset        String,
    total_volume Float64,
    trade_count  UInt32,
    avg_price    Decimal(18, 8),
    _computed_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(full_date)
ORDER BY (full_date, asset);

-- ── Pipeline Metadata (for dashboard) ────────────────────────────

CREATE TABLE IF NOT EXISTS etl_warehouse.pipeline_runs (
    run_id       String,
    dag_id       String,
    stage        String,  -- bronze, silver, gold, dbt
    status       String,  -- running, success, failed
    rows_read    UInt64 DEFAULT 0,
    rows_written UInt64 DEFAULT 0,
    started_at   DateTime,
    finished_at  Nullable(DateTime),
    error_msg    Nullable(String),
    _loaded_at   DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (started_at, dag_id);

-- ── LLM Insight Cache ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS etl_warehouse.llm_insights (
    insight_id   String,
    query_type   String,  -- text2sql, insight, anomaly
    user_query   String,
    generated_sql Nullable(String),
    response     String,
    model_used   String,
    latency_ms   UInt32,
    created_at   DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY created_at
TTL created_at + INTERVAL 30 DAY;

-- ── Seed: dim_sensor_location (hardcoded IoT locations) ──────────
INSERT INTO etl_warehouse.dim_sensor_location
    (location_id, site_name, latitude, longitude, sensor_type)
VALUES
    ('LOC-001', 'Warehouse A', 3.1390,  101.6869, 'multi'),
    ('LOC-002', 'Factory B',   1.3521,  103.8198, 'multi'),
    ('LOC-003', 'Office C',   13.7563,  100.5018, 'multi'),
    ('LOC-004', 'Depot D',    22.3193,  114.1694, 'multi'),
    ('LOC-005', 'Plant E',    37.5665,  126.9780, 'multi');

-- ── Seed: dim_date (2024-01-01 → 2027-12-31) ─────────────────────
INSERT INTO etl_warehouse.dim_date
    (date_key, full_date, year, quarter, month, week, day_of_week, is_weekend)
SELECT
    toUInt32(toYYYYMMDD(d))                        AS date_key,
    d                                              AS full_date,
    toYear(d)                                      AS year,
    toQuarter(d)                                   AS quarter,
    toMonth(d)                                     AS month,
    toISOWeek(d)                                   AS week,
    toDayOfWeek(d)                                 AS day_of_week,
    toUInt8(toDayOfWeek(d) IN (6, 7))              AS is_weekend
FROM (
    SELECT toDate('2024-01-01') + number AS d
    FROM numbers(1461)   -- 4 years (2024–2027), 2024 is a leap year
)
WHERE d <= toDate('2027-12-31');
