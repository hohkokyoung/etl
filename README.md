# ETL Platform — Local Data Engineering Stack

A fully offline, on-premise ETL platform that simulates an enterprise data engineering environment on a single machine. No cloud accounts needed.

## What This Is

This project lets you learn and demo the full data engineering lifecycle:

```
Simulated Data → Kafka → Data Lake (S3) → Data Warehouse (ClickHouse) → BI / LLM
```

Everything runs locally in Docker. "Mock AWS" is provided by LocalStack so you get real S3, SQS, SNS APIs without an AWS account.

---

## Quick Start

```bash
# 1. Start everything
make up

# 2. Initialize S3 buckets, SQS queues, SNS topics
make init-floci

# 3. Pull the local LLM model (2 GB download, one-time)
make pull-model

# 4. Start data generators (sends fake data into Kafka)
make sim-start

# 5. Check everything is healthy
make health
```

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Dashboard (React) | http://localhost:3030 | — |
| Dashboard API (FastAPI) | http://localhost:8000/docs | — |
| Airflow | http://localhost:8080 | admin / admin |
| Kafka UI | http://localhost:8082 | — |
| Spark UI | http://localhost:8083 | — |
| Grafana | http://localhost:3001 | admin / grafana_pass_2024 |
| Prometheus | http://localhost:9090 | — |
| LocalStack (mock AWS) | http://localhost:4566 | — |
| ClickHouse HTTP | http://localhost:8123 | etl_user / etl_pass_2024 |
| Ollama (local LLM) | http://localhost:11434 | — |

---

## Documentation

| Doc | What it covers |
|---|---|
| [Architecture](docs/01-architecture.md) | Why each component exists, how they relate |
| [Data Flow](docs/02-data-flow.md) | Step-by-step journey of data through the system |
| [Components](docs/03-components.md) | Deep dive into each service |
| [Running & Commands](docs/04-running.md) | All `make` commands explained |

---

## System Requirements

- Docker Desktop (WSL 2 backend on Windows)
- 16 GB RAM minimum, 32 GB recommended
- ~15 GB free disk (images) + ~5 GB for data volumes

## Cleanup

```bash
# Stop everything, keep data volumes
make down

# Stop everything AND delete all data
make reset

# Nuclear option — remove all images too
docker compose down --volumes --rmi all
docker builder prune -f
```
