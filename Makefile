.PHONY: up down reset sim-start sim-stop logs k8s-up k8s-down tf-init tf-apply pull-model health

# ── Docker Compose ────────────────────────────────────────────────
up:
	docker compose up -d --build
	@echo "Waiting for services to be healthy..."
	@sleep 10
	$(MAKE) init-floci
	$(MAKE) pull-model
	@echo ""
	@echo "✓ Stack is up. Access points:"
	@echo "  Dashboard  : http://localhost:3030"
	@echo "  API        : http://localhost:8000/docs"
	@echo "  Airflow    : http://localhost:8080  (admin/admin)"
	@echo "  Kafka UI   : http://localhost:8082"
	@echo "  Spark UI   : http://localhost:8083"
	@echo "  Grafana    : http://localhost:3001  (admin/grafana_pass_2024)"
	@echo "  Prometheus : http://localhost:9090"
	@echo "  ClickHouse : http://localhost:8123"
	@echo "  Floci      : http://localhost:4566  (mock AWS)"

down:
	docker compose down

reset:
	docker compose down -v
	@echo "All volumes removed. Run 'make up' to start fresh."

logs:
	docker compose logs -f --tail=50

logs-service:
	docker compose logs -f --tail=100 $(SERVICE)

# ── Floci Initialization ─────────────────────────────────────────
init-floci:
	@echo "Initializing Floci S3 buckets..."
	bash infra/floci/init.sh
	@echo "Floci initialized."

# ── LLM Model ────────────────────────────────────────────────────
pull-model:
	@echo "Pulling llama3.2:3b into Ollama (first run: ~2GB download)..."
	docker exec etl-ollama ollama pull llama3.2:3b
	@echo "Model ready."

# ── Simulation ───────────────────────────────────────────────────
sim-start:
	docker compose -f docker-compose.sim.yml up -d
	@echo "Simulators started. Watch Kafka UI at http://localhost:8082"

sim-stop:
	docker compose -f docker-compose.sim.yml down

sim-rate:
	@echo "Setting simulation rate to $(RATE) events/sec..."
	curl -s -X POST http://localhost:8000/api/simulation/rate -H "Content-Type: application/json" \
		-d '{"events_per_second": $(RATE)}' | python -m json.tool

# ── Health Check ─────────────────────────────────────────────────
health:
	@echo "=== Service Health ==="
	@curl -sf http://localhost:4566/_floci/health > /dev/null && echo "✓ Floci (mock AWS)" || echo "✗ Floci (mock AWS)"
	@docker exec etl-kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1 && echo "✓ Kafka" || echo "✗ Kafka"
	@curl -sf http://localhost:8081/subjects > /dev/null && echo "✓ Schema Registry" || echo "✗ Schema Registry"
	@docker exec etl-clickhouse clickhouse-client --query "SELECT 1" > /dev/null 2>&1 && echo "✓ ClickHouse" || echo "✗ ClickHouse"
	@docker exec etl-valkey valkey-cli ping > /dev/null 2>&1 && echo "✓ Valkey" || echo "✗ Valkey"
	@curl -sf http://localhost:11434/api/tags > /dev/null && echo "✓ Ollama" || echo "✗ Ollama"
	@curl -sf http://localhost:8000/health > /dev/null && echo "✓ Dashboard API" || echo "✗ Dashboard API"
	@curl -sf http://localhost:3030 > /dev/null && echo "✓ Dashboard UI" || echo "✗ Dashboard UI"

# ── dbt ──────────────────────────────────────────────────────────
dbt-run:
	cd processing/dbt && dbt run

dbt-test:
	cd processing/dbt && dbt test

dbt-docs:
	cd processing/dbt && dbt docs generate && dbt docs serve

# ── Kubernetes (k3d) ─────────────────────────────────────────────
k8s-cluster:
	k3d cluster create etl-cluster \
		--servers 1 \
		--agents 2 \
		--port "3000:30000@loadbalancer" \
		--port "8000:30001@loadbalancer" \
		--port "4566:30002@loadbalancer" \
		--wait
	@echo "k3d cluster 'etl-cluster' created with 1 server + 2 agents."

k8s-up:
	$(MAKE) k8s-cluster
	helm dependency update helm/
	helm upgrade --install etl-platform helm/ \
		--namespace etl-infra --create-namespace \
		-f helm/values-local.yaml \
		--timeout 10m \
		--wait
	@echo "ETL platform deployed to k3d."

k8s-down:
	k3d cluster delete etl-cluster

k8s-status:
	kubectl get pods -A | grep etl

# ── Terraform ────────────────────────────────────────────────────
tf-init:
	cd terraform/environments/local && terraform init

tf-plan:
	cd terraform/environments/local && terraform plan

tf-apply:
	cd terraform/environments/local && terraform apply -auto-approve

tf-destroy:
	cd terraform/environments/local && terraform destroy -auto-approve
