# ETL Platform — Complete Guide

A fully offline, on-premise ETL platform that simulates an enterprise data engineering environment on a single machine. No cloud accounts needed.

---

## Table of Contents

1. [Quick Start](#1-quick-start)
2. [Architecture](#2-architecture)
3. [Data Flow](#3-data-flow)
4. [Components](#4-components)
5. [Running & Commands](#5-running--commands)
6. [Troubleshooting](#6-troubleshooting)

---

## 1. Quick Start

```bash
# Start all 20 services
make up

# Initialize S3 buckets, SQS queues, SNS topics in LocalStack
make init-floci

# Pull the local LLM model (2 GB, one-time download)
make pull-model

# Start data generators (50 events/sec)
make sim-start

# Verify everything is healthy
make health
```

### Service URLs

| Service | URL | Credentials |
|---|---|---|
| Dashboard (React) | http://localhost:3030 | — |
| Dashboard API (FastAPI docs) | http://localhost:8000/docs | — |
| Airflow | http://localhost:8080 | admin / admin |
| Kafka UI | http://localhost:8082 | — |
| Spark UI | http://localhost:8083 | — |
| Grafana | http://localhost:3001 | admin / grafana_pass_2024 |
| Prometheus | http://localhost:9090 | — |
| LocalStack (mock AWS) | http://localhost:4566 | — |
| ClickHouse HTTP | http://localhost:8123 | etl_user / etl_pass_2024 |
| Ollama (local LLM) | http://localhost:11434 | — |

### System Requirements

- Docker Desktop (WSL 2 backend on Windows)
- 16 GB RAM minimum, 32 GB recommended
- ~15 GB free disk for images + ~5 GB for data volumes

---

## 2. Architecture

### The Big Picture

```
┌─────────────────────────────────────────────────────┐
│                  SIMULATION LAYER                    │
│   4 data generators producing fake business data    │
│   ecommerce · IoT sensors · financial · social      │
└──────────────────────┬──────────────────────────────┘
                       │ Kafka producers
                       ▼
┌─────────────────────────────────────────────────────┐
│                   KAFKA (KRaft)                      │
│   Message bus — decouples producers from consumers  │
│   Topics: transactions, sensors, financial, social  │
└──────────┬────────────────────────┬─────────────────┘
           │                        │
           ▼                        ▼
┌──────────────────┐   ┌────────────────────────────┐
│  Python Consumer │   │  Spark Structured Streaming │
│  raw JSON        │   │  typed + validated          │
│  → S3 bronze     │   │  → S3 silver (Iceberg)      │
└──────────┬───────┘   └────────────┬───────────────┘
           │                        │
           └────────────┬───────────┘
                        ▼
┌─────────────────────────────────────────────────────┐
│           DATA LAKE  —  LocalStack S3               │
│                                                     │
│   bronze/   raw Parquet, partitioned by date/hour   │
│   silver/   typed Iceberg tables, deduplicated      │
│   gold/     aggregated Iceberg marts, star schema   │
└──────────────────────┬──────────────────────────────┘
                       │ Spark batch jobs + dbt
                       ▼
┌─────────────────────────────────────────────────────┐
│         DATA WAREHOUSE  —  ClickHouse               │
│                                                     │
│   fact_sales · fact_sensor_readings                 │
│   fact_trades · fact_social_engagement              │
│   dim_customer · dim_product · dim_date             │
│   Materialized views for hourly/daily aggregates    │
└──────────────────────┬──────────────────────────────┘
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
    Power BI      LLM Layer     Dashboard
    (ODBC)    text-to-SQL    React + FastAPI
```

### Two Storage Layers — Why Both?

This is the **lakehouse pattern** — a common source of confusion.

| | Data Lake (S3) | Data Warehouse (ClickHouse) |
|---|---|---|
| **Purpose** | Source of truth, long-term archive | Fast queries for users and dashboards |
| **Format** | Parquet / Apache Iceberg | ClickHouse columnar format |
| **Accessed by** | Spark, Airflow, ML pipelines | Power BI, Dashboard, LLM |
| **Think of it as** | Giant cheap filing cabinet | Fast query engine for analytics |

They are **not duplicates** — S3 is the archive, ClickHouse is the speed layer. The pipeline loads gold data from S3 into ClickHouse.

### Why Each Component Exists

**Kafka (KRaft — no Zookeeper)**
Decouples producers from consumers. Generators don't care who's listening. Consumers don't care how fast data arrives. Kafka buffers everything in between. KRaft = modern Kafka without Zookeeper, one fewer service to run.

**Two Kafka Consumers**
| Consumer | Input | Output | Why |
|---|---|---|---|
| Python (confluent-kafka) | Raw JSON | Parquet on S3 bronze | Simple, fast, raw dump |
| Spark Structured Streaming | Same Kafka topics | Iceberg on S3 silver | Type enforcement, schema validation |

**Spark (1 master + 2 workers)**
Distributed processing engine. The master coordinates, workers do the compute. Even on one machine this teaches you the cluster model.

**dbt (data build tool)**
SQL transformation layer. Takes gold Iceberg data and models it into the star schema in ClickHouse. Handles dependencies, testing, and documentation automatically.

**Airflow (5 DAGs)**
Orchestrates everything on a schedule:
- bronze → silver (hourly)
- silver → gold (hourly)
- dbt run (every 2 hours)
- LLM insights (every 6 hours)
- ClickHouse optimize (3am daily)

**LLM Layer (3-tier fallback)**
```
Ollama llama3.2:3b  ←  primary (fully offline, always available)
        ↓ fails?
Groq API            ←  optional (add GROQ_API_KEY to .env)
        ↓ fails?
Rules engine        ←  always works (keyword matching, no AI needed)
```

**Celery + Valkey**
LLM inference is slow (2–5 seconds). The API can't block waiting. So:
1. API receives query → immediately returns a `job_id`
2. Celery worker picks it up asynchronously
3. Client polls or streams SSE until result is ready

Valkey = open-source Redis fork (Redis changed its license in 2024).

**Terraform + LocalStack**
Terraform normally provisions real AWS resources. Here it targets LocalStack instead. Same `.tf` files, same CLI — just `endpoint_url = http://localhost:4566`. Pure study/demo value.

**Helm + k3d**
The entire stack can also run on Kubernetes:
- **k3d** = k3s (lightweight Kubernetes) running inside Docker
- **Helm** = package manager for Kubernetes
- Run `make k8s-up` instead of `make up` to study the k8s architecture

---

## 3. Data Flow

How a single event travels from simulation to your dashboard.

### Step 1 — Data Generation

**Files:** `simulation/generators/`, `simulation/scheduler.py`

| Generator | What it makes | Kafka topic |
|---|---|---|
| `ecommerce.py` | Orders, products, customers | `transactions` |
| `iot_sensors.py` | Temperature, pressure, GPS readings | `sensors` |
| `financial.py` | Trades, balances, transfers | `financial` |
| `social.py` | Posts, likes, shares | `social` |

`scheduler.py` reads a target rate from Valkey (`sim:events_per_second`, default 50) and round-robins across all four generators. You can change the rate live from the dashboard without restarting anything.

```json
// Example sensor event published to Kafka
{
  "sensor_id": "sensor_042",
  "location": "warehouse-B",
  "temperature": 23.4,
  "pressure": 1013.2,
  "timestamp": "2024-01-15T10:30:00Z",
  "_anomaly": false
}
```

### Step 2 — Kafka (Message Bus)

Kafka receives all events and holds them for up to 24 hours. Two consumers read from it simultaneously and independently — Kafka tracks each consumer's position (offset) separately.

```
Generators → Kafka topics → [Python Consumer]
                          → [Spark Streaming]
```

The Schema Registry enforces Avro schemas so badly-formed messages are rejected before they pollute the lake.

### Step 3A — Python Consumer → Bronze (Raw Layer)

**Files:** `ingestion/consumers/python_raw/consumer.py`

Reads JSON from Kafka, batches it (5,000 rows or 30 seconds), writes Parquet to S3.

```
Kafka message (JSON)
    ↓ batch 5,000 rows or every 30 seconds
s3://raw-bronze/source=sensors/year=2024/month=01/day=15/hour=10/batch.parquet
```

**Why Parquet?** Columnar and compressed — 1 MB JSON becomes ~100 KB Parquet.  
**Why partition by date/hour?** Spark can skip entire folders. Query one hour = read one folder.

### Step 3B — Spark Streaming → Silver (Typed Layer)

**Files:** `ingestion/consumers/spark_streaming/streaming_job.py`

Every 30 seconds, Spark processes a micro-batch from Kafka:

```
Kafka messages (JSON strings)
    ↓ parse + enforce schema (correct types, reject nulls)
    ↓ add metadata columns (_batch_id, _processed_at)
    ↓ deduplicate (remove duplicate event IDs)
s3://processed-silver/sensors/   ← Iceberg table
```

**Why Iceberg?** ACID transactions, schema evolution, time travel. Silver is the **first trustworthy copy**.

### Step 4 — Spark Batch → Gold (Business Layer)

**Files:** `processing/spark_jobs/silver_to_gold.py`

Airflow triggers this hourly. Spark joins all silver tables and builds business aggregations:

```
silver/sensors + silver/transactions + silver/financial + silver/social
    ↓ join on shared keys (customer_id, date, location)
    ↓ apply business rules (revenue calculation, anomaly scoring)
    ↓ build star schema facts
s3://curated-gold/fact_sales/
s3://curated-gold/fact_sensor_readings/
s3://curated-gold/fact_trades/
s3://curated-gold/fact_social_engagement/
```

Gold is **query-ready** — modelled for analysts, not engineers.

### Step 5 — dbt → ClickHouse (Data Warehouse)

**Files:** `processing/dbt/models/`

dbt loads gold from S3 into ClickHouse with a full star schema:

```
S3 gold (Iceberg)
    ↓ staging models  (rename columns, cast types)
    ↓ intermediate    (business logic, joins)
    ↓ mart models     (final facts + dimensions)
ClickHouse: fact_sales, dim_customer, agg_daily_revenue, ...
```

dbt also runs `dbt test` — if data quality checks fail, Airflow marks the DAG as failed.

### Step 6 — Consumption (3 ways)

**A) Power BI Desktop** — connects via ODBC driver, DirectQuery mode for live data.

**B) LLM Text-to-SQL (AI Query Mode)**
```
User types: "show me top 5 products by revenue last week"
    ↓ FastAPI receives query
    ↓ Celery submits job to queue (returns job_id immediately)
    ↓ Celery worker calls Ollama (or Groq)
    ↓ LLM receives: schema context + user question
    ↓ LLM generates: SELECT ... FROM fact_sales WHERE ...
    ↓ Query executes against ClickHouse
    ↓ Result streams back to dashboard via SSE
Dashboard shows: table + auto-generated bar chart
```

**C) Grafana** — pre-built panels for pipeline health, Kafka lag, Spark jobs.

### Full Timeline of One Event

```
t=0ms    Generator creates a sensor reading
t=1ms    Kafka producer sends it to 'sensors' topic
t=30s    Python consumer writes Parquet batch to S3 bronze
t=30s    Spark streaming validates + writes to S3 silver
t=1hr    Airflow triggers Spark batch: silver → gold
t=2hr    Airflow triggers dbt run: gold → ClickHouse
t=2hr    Event is queryable in Power BI and AI mode
t=6hr    Airflow triggers LLM insight generation
t=6hr    Dashboard shows AI-generated summary
```

The pipeline is **eventually consistent** — ~2 hours from raw event to BI. Normal for batch ETL.

---

## 4. Components

### Infrastructure

#### Floci (Mock AWS)
**Image:** `floci/floci:latest` | **Port:** 4566 | **Config:** `infra/floci/init.sh`

Open-source AWS emulator (MIT license, no auth token needed). Supports 52 AWS services including S3, SQS, SNS, Glue, Athena, IAM, Kinesis. ~24ms startup, ~13MB idle RAM. Interact exactly like real AWS:
```bash
aws --endpoint-url http://localhost:4566 s3 ls
aws --endpoint-url http://localhost:4566 s3 ls s3://raw-bronze/ --recursive
```

#### Kafka (KRaft)
**Image:** `confluentinc/cp-kafka:7.7.0` | **Port:** 9092 | **UI:** http://localhost:8082

Topics: `transactions`, `sensors`, `financial`, `social`  
Listeners: `kafka:29092` (internal Docker) · `localhost:9092` (host machine)

#### Schema Registry
**Image:** `confluentinc/cp-schema-registry:7.7.0` | **Port:** 8081

Enforces Avro schemas. Rejects badly-formed messages before they enter the lake.

---

### Ingestion

#### Python Consumer
**Build:** `ingestion/consumers/python_raw/` | Python + confluent-kafka + pyarrow

Reads all 4 topics, batches messages, writes Parquet to S3 bronze partitioned by `source/year/month/day/hour/`.

#### Spark Structured Streaming
**Build:** `ingestion/consumers/spark_streaming/` | PySpark

30-second micro-batch triggers. Writes typed Iceberg tables to S3 silver.

---

### Processing

#### Spark Cluster
**Image:** `apache/spark:3.5.3` | **Ports:** 7077, 8083

| Container | Role | RAM limit |
|---|---|---|
| spark-master | Coordinates jobs + web UI | 4 GB |
| spark-worker-1 | Computation | 3 GB |
| spark-worker-2 | Computation | 3 GB |

Jobs: `bronze_to_silver.py` (clean + type), `silver_to_gold.py` (aggregate + model)

#### dbt
**Files:** `processing/dbt/` | **Target:** ClickHouse

```
staging/       rename columns, basic casts
    ↓
intermediate/  joins, business logic
    ↓
marts/         final facts and dimensions for BI
```

---

### Storage

#### ClickHouse
**Image:** `clickhouse/clickhouse-server:24.8` | **Ports:** 8123 (HTTP), 9000 (native)

```
Facts:       fact_sales, fact_sensor_readings, fact_trades, fact_social_engagement
Dimensions:  dim_customer, dim_product, dim_sensor_location, dim_date
Aggregates:  agg_daily_revenue, agg_hourly_sensor, agg_trading_volume
Meta:        pipeline_runs, llm_insights
```

```bash
curl "http://localhost:8123/?query=SELECT+count()+FROM+etl_warehouse.fact_sales" \
  -u etl_user:etl_pass_2024
```

#### PostgreSQL
**Image:** `postgres:16-alpine` | **Port:** 5432

- `airflow` — Airflow metadata (DAG runs, task state)
- `oltp_source` — simulated OLTP source data

---

### LLM Layer

#### Ollama
**Image:** `ollama/ollama:latest` | **Port:** 11434 | Model: `llama3.2:3b` (~2 GB)

Runs LLMs locally on CPU. No GPU required. ~2–5 seconds per query.
```bash
make pull-model
# or: docker exec etl-ollama ollama pull llama3.2:3b
```

#### Celery Worker
**Build:** `llm/` | **Broker:** Valkey

Async LLM job processor. API submits → returns `job_id` → worker runs inference → client polls/SSE.

#### Valkey
**Image:** `valkey/valkey:8-alpine` | **Port:** 6379

Redis-compatible open-source fork. Used for:
- API response cache (10s / 30s TTL)
- Celery task broker (LLM job queue)
- Simulation rate config (`sim:events_per_second`)
- Pub/sub for live dashboard updates

---

### Orchestration & Monitoring

#### Apache Airflow
**Image:** `apache/airflow:2.9.3` | **Port:** 8080 | Login: admin / admin

| DAG | Schedule | What it does |
|---|---|---|
| `dag_bronze_to_silver` | Hourly | Spark: raw Parquet → typed Iceberg |
| `dag_silver_to_gold` | Hourly | Spark: typed → aggregated marts |
| `dag_dbt_run` | Every 2h | dbt run + dbt test on ClickHouse |
| `dag_llm_insights` | Every 6h | LLM summarizes last 6h of data |
| `dag_clickhouse_optimize` | 3am daily | OPTIMIZE TABLE FINAL |

#### Prometheus + Grafana
**Prometheus:** port 9090 — scrapes all services every 15s, retains 7 days  
**Grafana:** port 3001 — pre-built dashboards for pipeline health, Kafka lag, Spark jobs

---

### Dashboard

#### FastAPI Backend
**Build:** `dashboard/api/` | **Port:** 8000 | **Docs:** http://localhost:8000/docs

| Endpoint | What it returns |
|---|---|
| `GET /health` | All service statuses |
| `GET /api/pipeline/topology` | Node/edge data for React Flow graph |
| `GET /api/pipeline/health` | Kafka lag, throughput (cached 10s) |
| `GET /api/lake/stats` | Row counts per S3 layer (cached 30s) |
| `GET /api/simulation/rate` | Current events/sec |
| `POST /api/simulation/rate` | Change events/sec |
| `POST /api/insights/query` | Submit LLM query → returns job_id |
| `GET /api/insights/stream/{id}` | SSE stream for result |
| `GET /api/insights/history` | Last 20 AI queries |

#### React Frontend
**Build:** `dashboard/frontend/` | **Port:** 3030

- **Overview** — live pipeline topology (React Flow), service health nodes, animated data edges
- **Simulation** — start/stop generators, live events/sec slider, per-topic counters
- **Insights** — natural language query → Text-to-SQL / Insight / Anomaly Detection → table + chart

---

### IaC & Kubernetes (Study Components)

#### Terraform
**Files:** `terraform/`

```bash
make tf-init    # terraform init (S3 backend in LocalStack)
make tf-plan    # preview changes
make tf-apply   # provision S3, SQS, SNS, IAM via LocalStack
make tf-destroy # tear down
```

Same `.tf` files work against real AWS — just change `endpoint_url` in `environments/prod/`.

#### Helm + k3d (Kubernetes)
**Files:** `helm/`

```bash
make k8s-cluster   # create k3d cluster (1 control plane + 2 workers in Docker)
make k8s-up        # helm install full stack into k8s
make k8s-down      # helm uninstall + delete cluster
```

Each service has its own Helm sub-chart with HPA (auto-scaling) configured.

---

## 5. Running & Commands

### All Make Commands

| Command | What it does |
|---|---|
| `make up` | Start all services |
| `make down` | Stop all, keep data volumes |
| `make reset` | Stop all, delete data volumes |
| `make logs` | Tail logs from all services |
| `make health` | Check which services are healthy |
| `make init-floci` | Create S3 buckets, SQS, SNS in LocalStack |
| `make pull-model` | Pull `llama3.2:3b` into Ollama (~2 GB) |
| `make sim-start` | Start all 4 data generators |
| `make sim-stop` | Stop data generators |
| `make sim-rate RATE=100` | Change events/sec live |
| `make dbt-run` | Run dbt models manually |
| `make dbt-test` | Run dbt data quality tests |
| `make k8s-cluster` | Create k3d cluster |
| `make k8s-up` | Deploy via Helm |
| `make k8s-down` | Tear down k8s |
| `make tf-init` | Terraform init |
| `make tf-plan` | Terraform plan |
| `make tf-apply` | Terraform apply |
| `make tf-destroy` | Terraform destroy |

### Startup Order

```
1. postgres, valkey, clickhouse        (no dependencies)
2. kafka, floci/LocalStack             (no dependencies)
3. schema-registry, kafka-ui           (wait: kafka healthy)
4. spark-master                        (no dependencies)
5. spark-worker-1, spark-worker-2      (wait: spark-master healthy)
6. ollama                              (no dependencies, 60s start period)
7. airflow-init                        (wait: postgres healthy → runs once + exits)
8. airflow-webserver, airflow-scheduler (wait: airflow-init complete)
9. python-consumer                     (wait: kafka + floci healthy)
10. celery-worker                      (wait: valkey + ollama + clickhouse healthy)
11. dashboard-api                      (wait: valkey + clickhouse + kafka healthy)
12. dashboard-ui                       (wait: dashboard-api)
13. prometheus, grafana                (no strict dependencies)
```

Total from `make up` to fully healthy: **~2–3 minutes**.

### Useful Direct Commands

```bash
# View logs for a specific service
docker compose logs -f kafka
docker compose logs -f celery-worker

# Query ClickHouse interactively
docker exec -it etl-clickhouse clickhouse-client --user etl_user --password etl_pass_2024

# List Kafka topics
docker exec etl-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Peek at Kafka messages
docker exec etl-kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 --topic sensors --max-messages 5

# List S3 bronze files
aws --endpoint-url http://localhost:4566 s3 ls s3://raw-bronze/ --recursive

# Submit a Spark job manually
docker exec etl-spark-master \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-jobs/bronze_to_silver.py
```

### Key .env Variables

```bash
# Add Groq API key to enable cloud LLM fallback (free tier available)
GROQ_API_KEY=your_key_here

# Change simulation rate (default 50 events/sec)
SIM_EVENTS_PER_SECOND=100

# Use smaller model to save RAM
OLLAMA_MODEL=llama3.2:1b

# Fallback to rules engine faster if queue fills
LLM_QUEUE_MAX_DEPTH=3
```

---

## 6. Troubleshooting

**Service keeps restarting**
```bash
docker logs etl-<service-name> --tail 30
```

**Kafka topics not found on startup**
Normal — topics are auto-created on first message. Run `make sim-start` first, then start consumers.

**Ollama marked unhealthy**
Ollama is running but no model is loaded yet. Run `make pull-model`. The LLM rules engine fallback still works without a model.

**Port already in use**
```powershell
netstat -ano | findstr :PORT_NUMBER   # find the conflicting process
```
Dashboard UI moved to port 3030 because Docker Desktop uses 3000 internally.

**Out of memory**
```bash
make sim-rate RATE=10          # reduce event throughput
# or remove spark-worker-2 from docker-compose.yml to save 3 GB
```

**Reset and start fresh**
```bash
make reset       # deletes volumes (all data gone)
make up
make init-floci  # recreate S3 buckets
make sim-start   # restart generators
```

**Nuclear cleanup (remove all images too)**
```bash
docker compose down --volumes --rmi all
docker builder prune -f
```
