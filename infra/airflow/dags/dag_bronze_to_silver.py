"""DAG: bronze → silver (hourly, runs PySpark locally in Airflow)."""
from datetime import timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

SOURCES = ["transactions", "sensors", "financial", "social"]

JARS = ",".join([
    "/opt/spark-jars/iceberg-spark-runtime-3.5_2.12-1.6.0.jar",
    "/opt/spark-jars/hadoop-aws-3.3.4.jar",
    "/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar",
])


def run_bronze_to_silver(source: str, ds: str, **context):
    import sys
    sys.path.insert(0, "/opt/airflow/spark_jobs")

    os.environ["ETL_SOURCE"] = source
    os.environ["ETL_DATE"] = ds
    os.environ["AWS_ENDPOINT_URL"] = "http://floci:4566"
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {JARS} pyspark-shell"

    # Import and run directly
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "bronze_to_silver", "/opt/airflow/spark_jobs/bronze_to_silver.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.process(source, ds)  # call process(source, date) directly


with DAG(
    dag_id="dag_bronze_to_silver",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5), "owner": "etl"},
    tags=["etl", "spark", "bronze", "silver"],
) as dag:

    for source in SOURCES:
        PythonOperator(
            task_id=f"spark_{source}",
            python_callable=run_bronze_to_silver,
            op_kwargs={"source": source},
        )
