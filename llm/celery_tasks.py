"""
Celery async LLM tasks — decouples slow LLM inference from the API request cycle.
The API submits a task, returns a job_id immediately, and the client polls/SSE streams.
"""
import json
import logging
import os

import clickhouse_connect
from celery import Celery

from analyzer import AnalysisResult, QueryType, analyze

logger = logging.getLogger(__name__)

VALKEY_URL = os.environ.get("VALKEY_URL", "redis://localhost:6379/0")

app = Celery("etl_llm", broker=VALKEY_URL, backend=VALKEY_URL)
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
    task_soft_time_limit=90,
    task_time_limit=120,
    worker_prefetch_multiplier=1,  # one task at a time per worker (Ollama is serial)
)


def _get_ch():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "etl_user"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "etl_pass_2024"),
        database=os.environ.get("CLICKHOUSE_DB", "etl_warehouse"),
    )


@app.task(bind=True, name="llm.analyze_query")
def analyze_query(self, user_query: str, query_type: str = "text2sql") -> dict:
    """
    Run LLM analysis. If text2sql, also execute the generated SQL and return rows.
    Returns a dict serialisable to JSON.
    """
    qt = QueryType(query_type)
    result: AnalysisResult = analyze(user_query, qt)

    rows: list[dict] | None = None
    columns: list[str] | None = None
    exec_error: str | None = None

    if result.generated_sql and qt == QueryType.TEXT_TO_SQL:
        try:
            client = _get_ch()
            qr = client.query(result.generated_sql)
            columns = list(qr.column_names)
            rows = [dict(zip(columns, row)) for row in qr.result_rows[:500]]
        except Exception as exc:
            exec_error = str(exc)
            logger.warning("SQL execution failed: %s", exc)

    # Persist to llm_insights table
    try:
        client = _get_ch()
        client.insert(
            "llm_insights",
            [[
                result.insight_id, result.query_type, result.user_query,
                result.generated_sql, result.response, result.model_used,
                result.latency_ms,
            ]],
            column_names=["insight_id", "query_type", "user_query", "generated_sql",
                          "response", "model_used", "latency_ms"],
        )
    except Exception as exc:
        logger.warning("Failed to persist insight: %s", exc)

    return {
        "insight_id": result.insight_id,
        "query_type": result.query_type,
        "user_query": result.user_query,
        "generated_sql": result.generated_sql,
        "response": result.response,
        "model_used": result.model_used,
        "latency_ms": result.latency_ms,
        "rows": rows,
        "columns": columns,
        "exec_error": exec_error,
        "error": result.error,
    }
