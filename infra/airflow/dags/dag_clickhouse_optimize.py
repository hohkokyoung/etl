"""DAG: optimize ClickHouse SummingMergeTree tables (nightly)."""
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

TABLES_TO_OPTIMIZE = [
    "agg_daily_revenue",
    "agg_hourly_sensor",
    "agg_trading_volume",
]


def optimize_tables(**context):
    import os
    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        username="etl_user",
        password="etl_pass_2024",
        database="etl_warehouse",
    )
    for table in TABLES_TO_OPTIMIZE:
        client.command(f"OPTIMIZE TABLE etl_warehouse.{table} FINAL")
        print(f"Optimized {table}")


with DAG(
    dag_id="dag_clickhouse_optimize",
    schedule_interval="0 3 * * *",  # 3am daily
    start_date=days_ago(1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5), "owner": "etl"},
    tags=["etl", "clickhouse", "maintenance"],
) as dag:

    PythonOperator(
        task_id="optimize_agg_tables",
        python_callable=optimize_tables,
    )
