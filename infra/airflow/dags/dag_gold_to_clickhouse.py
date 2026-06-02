"""DAG: gold (S3 Iceberg) → ClickHouse (every 2 hours).
Uses ClickHouse S3 table function to read gold Parquet files directly —
no extra Spark job or JDBC driver needed.
"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

S3_ENDPOINT = "http://floci:4566"
S3_GOLD     = "curated-gold"
CH_HOST     = "clickhouse"
CH_USER     = "etl_user"
CH_PASSWORD = "etl_pass_2024"
CH_DB       = "etl_warehouse"

# Fact tables: loaded by date partition (idempotent per date_key)
FACT_TABLES = {
    "fact_sales":             "fact_sales",
    "fact_sensor_readings":   "fact_sensor_readings",
    "fact_trades":            "fact_trades",
    "fact_social_engagement": "fact_social_engagement",
}

# Dim tables: full replace on every run (no date partitioning)
DIM_TABLES = {
    "dim_customer": "dim_customer",
    "dim_product":  "dim_product",
}


def load_gold_to_clickhouse(ds: str, **context):
    import clickhouse_connect

    ch = clickhouse_connect.get_client(
        host=CH_HOST,
        port=8123,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
    )

    date_key = ds.replace("-", "")  # "2026-06-01" → "20260601"

    # ── Fact tables: date-partitioned, idempotent per date_key ───────
    for gold_table, ch_table in FACT_TABLES.items():
        s3_path = (
            f"{S3_ENDPOINT}/{S3_GOLD}/default/{gold_table}/"
            f"data/date_key={date_key}/*.parquet"
        )
        print(f"Loading fact {gold_table} → {ch_table} from {s3_path}")
        ch.command(f"ALTER TABLE {ch_table} DELETE WHERE date_key = {date_key}")
        ch.command(f"""
            INSERT INTO {ch_table}
            SELECT * FROM s3('{s3_path}', 'test', 'test', 'Parquet')
        """)
        count = ch.query(
            f"SELECT count() FROM {ch_table} WHERE date_key = {date_key}"
        ).result_rows[0][0]
        print(f"  ✓ {ch_table}: {count:,} rows loaded for date_key={date_key}")

    # ── Dim tables: full replace (no date partition) ──────────────────
    for gold_table, ch_table in DIM_TABLES.items():
        s3_path = f"{S3_ENDPOINT}/{S3_GOLD}/default/{gold_table}/data/*.parquet"
        print(f"Loading dim {gold_table} → {ch_table} from {s3_path}")
        ch.command(f"TRUNCATE TABLE {ch_table}")
        ch.command(f"""
            INSERT INTO {ch_table}
            SELECT * FROM s3('{s3_path}', 'test', 'test', 'Parquet')
        """)
        count = ch.query(f"SELECT count() FROM {ch_table}").result_rows[0][0]
        print(f"  ✓ {ch_table}: {count:,} rows loaded")

    # ── OPTIMIZE all tables ───────────────────────────────────────────
    for ch_table in list(FACT_TABLES.values()) + list(DIM_TABLES.values()):
        ch.command(f"OPTIMIZE TABLE {ch_table} FINAL")
        print(f"  ✓ OPTIMIZE {ch_table} done")


with DAG(
    dag_id="dag_gold_to_clickhouse",
    schedule_interval="0 */2 * * *",   # every 2 hours
    start_date=days_ago(1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5), "owner": "etl"},
    tags=["etl", "clickhouse", "gold"],
) as dag:

    PythonOperator(
        task_id="load_gold_to_clickhouse",
        python_callable=load_gold_to_clickhouse,
    )
