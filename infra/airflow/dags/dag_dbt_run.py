"""DAG: dbt run + test (runs after gold is ready, every 2 hours)."""
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="dag_dbt_run",
    schedule_interval="0 */2 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10), "owner": "etl"},
    tags=["etl", "dbt", "gold", "clickhouse"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/dbt && dbt run --profiles-dir /opt/dbt --target dev",
        env={
            "CLICKHOUSE_HOST": "clickhouse",
            "CLICKHOUSE_USER": "etl_user",
            "CLICKHOUSE_PASSWORD": "etl_pass_2024",
        },
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/dbt && dbt test --profiles-dir /opt/dbt --target dev",
        env={
            "CLICKHOUSE_HOST": "clickhouse",
            "CLICKHOUSE_USER": "etl_user",
            "CLICKHOUSE_PASSWORD": "etl_pass_2024",
        },
    )

    dbt_run >> dbt_test
