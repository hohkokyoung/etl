"""DAG: silver → gold (hourly, runs PySpark locally in Airflow)."""
from datetime import timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

JARS = ",".join([
    "/opt/spark-jars/iceberg-spark-runtime-3.5_2.12-1.6.0.jar",
    "/opt/spark-jars/hadoop-aws-3.3.4.jar",
    "/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar",
])


def run_silver_to_gold(ds: str, **context):
    os.environ["ETL_DATE"] = ds
    os.environ["AWS_ENDPOINT_URL"] = "http://floci:4566"
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {JARS} pyspark-shell"

    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "silver_to_gold", "/opt/airflow/spark_jobs/silver_to_gold.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.process(ds)  # call process(date) directly


with DAG(
    dag_id="dag_silver_to_gold",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5), "owner": "etl"},
    tags=["etl", "spark", "silver", "gold"],
) as dag:

    PythonOperator(
        task_id="spark_silver_to_gold",
        python_callable=run_silver_to_gold,
    )
