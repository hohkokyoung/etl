"""DAG: silver → gold (hourly Spark batch, all sources in one job)."""
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="dag_silver_to_gold",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5), "owner": "etl"},
    tags=["etl", "spark", "silver", "gold"],
) as dag:

    BashOperator(
        task_id="spark_silver_to_gold",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4 "
            "/opt/spark-jobs/silver_to_gold.py "
            "--date {{ ds }}"
        ),
        env={
            "AWS_ENDPOINT_URL": "http://floci:4566",
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
        },
    )
