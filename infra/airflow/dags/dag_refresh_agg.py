"""
DAG: refresh ClickHouse aggregation tables from fact tables (hourly).

Truncates and rebuilds agg_daily_revenue, agg_hourly_sensor, agg_trading_volume
by joining fact tables with dim tables already in ClickHouse.
This is the correct automated replacement for the one-time manual backfill.
"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

CH_HOST     = "clickhouse"
CH_USER     = "etl_user"
CH_PASSWORD = "etl_pass_2024"
CH_DB       = "etl_warehouse"


def refresh_agg_tables(**context):
    import clickhouse_connect

    ch = clickhouse_connect.get_client(
        host=CH_HOST, port=8123,
        username=CH_USER, password=CH_PASSWORD,
        database=CH_DB,
    )

    # ── agg_daily_revenue ────────────────────────────────────────────
    # Rebuild only the last 2 days to keep it fast; idempotent.
    print("Refreshing agg_daily_revenue...")
    ch.command("ALTER TABLE agg_daily_revenue DELETE WHERE full_date >= today() - 2")
    ch.command("""
        INSERT INTO agg_daily_revenue
            (date_key, full_date, region, category, total_orders, total_revenue, avg_order_value)
        SELECT
            fs.date_key,
            toDate(fs.event_ts)       AS full_date,
            dc.region,
            dp.category,
            count()                   AS total_orders,
            sum(fs.total_amount)      AS total_revenue,
            avg(fs.total_amount)      AS avg_order_value
        FROM fact_sales fs
        JOIN dim_customer dc ON fs.customer_id = dc.customer_id
        JOIN dim_product  dp ON fs.product_id  = dp.product_id
        WHERE toDate(fs.event_ts) >= today() - 2
        GROUP BY fs.date_key, full_date, dc.region, dp.category
    """)
    count = ch.query("SELECT count() FROM agg_daily_revenue").result_rows[0][0]
    print(f"  ✓ agg_daily_revenue: {count:,} rows")

    # ── agg_hourly_sensor ────────────────────────────────────────────
    print("Refreshing agg_hourly_sensor...")
    ch.command("ALTER TABLE agg_hourly_sensor DELETE WHERE hour_ts >= now() - INTERVAL 2 HOUR")
    ch.command("""
        INSERT INTO agg_hourly_sensor
            (hour_ts, location_id, avg_temp, avg_pressure, avg_humidity, reading_count, anomaly_flag)
        SELECT
            toStartOfHour(event_ts)   AS hour_ts,
            location_id,
            avg(temperature)          AS avg_temp,
            avg(pressure)             AS avg_pressure,
            avg(humidity)             AS avg_humidity,
            count()                   AS reading_count,
            toUInt8(avg(temperature) > 35 OR avg(temperature) < 10) AS anomaly_flag
        FROM fact_sensor_readings
        WHERE event_ts >= now() - INTERVAL 2 HOUR
        GROUP BY hour_ts, location_id
    """)
    count = ch.query("SELECT count() FROM agg_hourly_sensor").result_rows[0][0]
    print(f"  ✓ agg_hourly_sensor: {count:,} rows")

    # ── agg_trading_volume ───────────────────────────────────────────
    print("Refreshing agg_trading_volume...")
    ch.command("ALTER TABLE agg_trading_volume DELETE WHERE full_date >= today() - 2")
    ch.command("""
        INSERT INTO agg_trading_volume
            (date_key, full_date, asset, total_volume, trade_count, avg_price)
        SELECT
            date_key,
            toDate(event_ts)          AS full_date,
            asset,
            sum(quantity)             AS total_volume,
            count()                   AS trade_count,
            avg(price)                AS avg_price
        FROM fact_trades
        WHERE toDate(event_ts) >= today() - 2
        GROUP BY date_key, full_date, asset
    """)
    count = ch.query("SELECT count() FROM agg_trading_volume").result_rows[0][0]
    print(f"  ✓ agg_trading_volume: {count:,} rows")

    ch.command("OPTIMIZE TABLE agg_daily_revenue FINAL")
    ch.command("OPTIMIZE TABLE agg_hourly_sensor FINAL")
    ch.command("OPTIMIZE TABLE agg_trading_volume FINAL")
    print("Done.")


with DAG(
    dag_id="dag_refresh_agg",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2), "owner": "etl"},
    tags=["etl", "clickhouse", "agg"],
) as dag:

    PythonOperator(
        task_id="refresh_agg_tables",
        python_callable=refresh_agg_tables,
    )
