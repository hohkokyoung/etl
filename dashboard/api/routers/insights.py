"""
LLM insights router.
POST /api/insights/query  → submits Celery task, returns job_id immediately
GET  /api/insights/result/{job_id} → poll result
GET  /api/insights/stream/{job_id} → SSE stream that fires when done
GET  /api/insights/history → last 20 queries from ClickHouse
"""
import asyncio
import json
import os
from typing import AsyncGenerator

import clickhouse_connect
from celery.result import AsyncResult
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from celery import Celery

router = APIRouter()

# Broker-only Celery app — no tasks defined here, just connects to the same
# Valkey broker so we can send tasks to the celery-worker container.
VALKEY_URL = os.environ.get("VALKEY_URL", "redis://localhost:6379/0")
celery_app = Celery("etl_api_client", broker=VALKEY_URL, backend=VALKEY_URL)


class QueryRequest(BaseModel):
    query: str
    query_type: str = "text2sql"  # text2sql | insight | anomaly


def _ch():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "etl_user"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "etl_pass_2024"),
        database="etl_warehouse",
    )


@router.post("/query")
async def submit_query(req: QueryRequest):
    task = celery_app.send_task(
        "llm.analyze_query",
        args=[req.query, req.query_type],
    )
    return {"job_id": task.id, "status": "submitted"}


@router.get("/result/{job_id}")
async def get_result(job_id: str):
    result = AsyncResult(job_id, app=celery_app)
    if result.ready():
        return {"status": "done", "result": result.get()}
    return {"status": result.state.lower(), "result": None}


async def _sse_generator(job_id: str) -> AsyncGenerator[str, None]:
    result = AsyncResult(job_id, app=celery_app)
    # Send heartbeat every 2s while pending
    while not result.ready():
        yield f"data: {json.dumps({'status': 'pending'})}\n\n"
        await asyncio.sleep(2)
    yield f"data: {json.dumps({'status': 'done', 'result': result.get()})}\n\n"


@router.get("/stream/{job_id}")
async def stream_result(job_id: str):
    return StreamingResponse(
        _sse_generator(job_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/history")
async def insight_history():
    try:
        ch = _ch()
        qr = ch.query(
            "SELECT insight_id, query_type, user_query, response, model_used, "
            "latency_ms, created_at FROM etl_warehouse.llm_insights "
            "ORDER BY created_at DESC LIMIT 20"
        )
        cols = list(qr.column_names)
        return {"rows": [dict(zip(cols, row)) for row in qr.result_rows]}
    except Exception as exc:
        return {"rows": [], "error": str(exc)}
