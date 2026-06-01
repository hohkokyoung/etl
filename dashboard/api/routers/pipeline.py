"""
Pipeline health router — Kafka lag, service status, throughput metrics.
Results are cached in Valkey to avoid hammering Kafka on every dashboard refresh.
"""
import os
from typing import Any

from confluent_kafka.admin import AdminClient
from fastapi import APIRouter

from cache import get, set as cache_set

router = APIRouter()

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPICS = ["transactions", "sensors", "financial", "social"]
CACHE_TTL = int(os.environ.get("API_CACHE_TTL_PIPELINE", "10"))


def _kafka_admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": KAFKA_SERVERS})


def _get_kafka_stats() -> dict[str, Any]:
    try:
        admin = _kafka_admin()
        metadata = admin.list_topics(timeout=5)
        topics_info = {}
        for topic in TOPICS:
            if topic in metadata.topics:
                parts = metadata.topics[topic].partitions
                topics_info[topic] = {"partitions": len(parts), "status": "ok"}
            else:
                topics_info[topic] = {"partitions": 0, "status": "missing"}
        return {"kafka": "healthy", "topics": topics_info}
    except Exception as exc:
        return {"kafka": "unavailable", "error": str(exc)}


def _get_service_statuses() -> list[dict]:
    import httpx
    services = [
        {"name": "Floci",     "url": "http://floci:4566/_floci/health",  "port": 4566},
        {"name": "Kafka UI",  "url": "http://kafka-ui:8080/actuator/health", "port": 8082},
        {"name": "Spark",     "url": "http://spark-master:8080",          "port": 8083},
        {"name": "Airflow",   "url": "http://airflow-webserver:8080/health", "port": 8080},
        {"name": "Ollama",    "url": "http://ollama:11434/api/tags",      "port": 11434},
        {"name": "ClickHouse","url": "http://clickhouse:8123/ping",       "port": 8123},
    ]
    statuses = []
    with httpx.Client(timeout=2.0) as client:
        for svc in services:
            try:
                r = client.get(svc["url"])
                statuses.append({"name": svc["name"], "port": svc["port"],
                                  "status": "healthy" if r.status_code < 400 else "degraded"})
            except Exception:
                statuses.append({"name": svc["name"], "port": svc["port"], "status": "unreachable"})
    return statuses


@router.get("/health")
async def pipeline_health():
    cached = await get("pipeline:health")
    if cached:
        return cached

    data = {
        "kafka": _get_kafka_stats(),
        "services": _get_service_statuses(),
    }
    await cache_set("pipeline:health", data, ttl=CACHE_TTL)
    return data


@router.get("/topology")
async def pipeline_topology():
    """Returns node+edge data for the React Flow pipeline graph."""
    nodes = [
        {"id": "sim",      "label": "Simulators",    "type": "source",    "x": 50,  "y": 200},
        {"id": "kafka",    "label": "Kafka (KRaft)",  "type": "stream",    "x": 250, "y": 200},
        {"id": "py_con",   "label": "Python Consumer","type": "consumer",  "x": 450, "y": 100},
        {"id": "spark_con","label": "Spark Streaming","type": "consumer",  "x": 450, "y": 300},
        {"id": "floci",    "label": "Floci S3",       "type": "storage",   "x": 650, "y": 200},
        {"id": "spark_b",  "label": "Spark Batch",    "type": "processing","x": 850, "y": 200},
        {"id": "dbt",      "label": "dbt",            "type": "processing","x": 1050,"y": 200},
        {"id": "ch",       "label": "ClickHouse",     "type": "warehouse", "x": 1250,"y": 200},
        {"id": "llm",      "label": "LLM (Ollama)",   "type": "ai",        "x": 1250,"y": 350},
        {"id": "dash",     "label": "Dashboard",      "type": "output",    "x": 1450,"y": 200},
        {"id": "powerbi",  "label": "Power BI",       "type": "output",    "x": 1450,"y": 350},
        {"id": "grafana",  "label": "Grafana",        "type": "output",    "x": 1450,"y": 100},
    ]
    edges = [
        {"source": "sim",       "target": "kafka"},
        {"source": "kafka",     "target": "py_con"},
        {"source": "kafka",     "target": "spark_con"},
        {"source": "py_con",    "target": "floci",   "label": "bronze"},
        {"source": "spark_con", "target": "floci",   "label": "silver"},
        {"source": "floci",     "target": "spark_b"},
        {"source": "spark_b",   "target": "floci",   "label": "gold"},
        {"source": "floci",     "target": "dbt",     "label": "gold→CH"},
        {"source": "dbt",       "target": "ch"},
        {"source": "ch",        "target": "llm"},
        {"source": "ch",        "target": "dash"},
        {"source": "ch",        "target": "powerbi"},
        {"source": "llm",       "target": "dash"},
        {"source": "ch",        "target": "grafana"},
    ]
    return {"nodes": nodes, "edges": edges}
