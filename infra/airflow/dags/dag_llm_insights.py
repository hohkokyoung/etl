"""DAG: scheduled LLM insight generation (every 6 hours)."""
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

INSIGHT_QUERIES = [
    ("insight", "Summarise today's revenue performance, highlighting regions above or below average."),
    ("anomaly", "Identify any IoT sensor locations with unusual temperature or pressure readings in the last 6 hours."),
    ("insight", "What are the top 3 trending assets by trading volume today vs. yesterday?"),
    ("insight", "Which social media platforms show the highest engagement growth this week?"),
]


def run_insights(**context):
    import os, sys
    sys.path.insert(0, "/opt/airflow")
    from llm.celery_tasks import analyze_query
    for qt, query in INSIGHT_QUERIES:
        task = analyze_query.delay(query, qt)
        print(f"Submitted {qt} task {task.id}: {query[:60]}")


with DAG(
    dag_id="dag_llm_insights",
    schedule_interval="0 */6 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args={"retries": 0, "owner": "etl"},
    tags=["etl", "llm", "insights"],
) as dag:

    PythonOperator(
        task_id="submit_llm_insights",
        python_callable=run_insights,
        env_vars={
            "OLLAMA_BASE_URL": "http://ollama:11434",
            "VALKEY_URL": "redis://valkey:6379/0",
            "CLICKHOUSE_HOST": "clickhouse",
        },
    )
