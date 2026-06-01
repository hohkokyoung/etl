"""DAG: bronze → silver (hourly Spark batch per source)."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

SOURCES = ["transactions", "sensors", "financial", "social"]

with DAG(
    dag_id="dag_bronze_to_silver",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "owner": "etl",
    },
    tags=["etl", "spark", "bronze", "silver"],
) as dag:

    for source in SOURCES:
        BashOperator(
            task_id=f"spark_{source}",
            bash_command=(
                "spark-submit "
                "--master spark://spark-master:7077 "
                "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4 "
                "/opt/spark-jobs/bronze_to_silver.py "
                f"--source {source} "
                "--date {{ ds }}"
            ),
            env={
                "AWS_ENDPOINT_URL": "http://floci:4566",
                "AWS_ACCESS_KEY_ID": "test",
                "AWS_SECRET_ACCESS_KEY": "test",
                "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            },
        )
