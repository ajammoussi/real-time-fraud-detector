.PHONY: setup infra-up infra-down infra-rebuild seed features data train test serve monitor drift all clean bootstrap-model ensure-model ensure-mlflow-db logs db-upgrade db-revision

# Enable Docker BuildKit for faster caching (must be run in terminal or .env)
export DOCKER_BUILDKIT=1

COMPOSE_FILE=docker/docker-compose.yml
COMPOSE=docker compose -f $(COMPOSE_FILE)
# Host-side Python for `test`/`db-revision`. Override when the venv is not
# activated, e.g. `make test PYTHON=.venv/Scripts/python.exe`.
PYTHON ?= python

setup:
	pip install -r requirements/dev.txt
	@[ -f .env ] || cp .env.example .env
	pre-commit install

ensure-mlflow-db:
	@echo "==> Ensuring MLflow metadata database exists (idempotent)..."
	@$(COMPOSE) exec -T postgres psql -U mlops -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='mlflow'" | grep -q 1 \
		|| $(COMPOSE) exec -T postgres psql -U mlops -d postgres -c "CREATE DATABASE mlflow"

infra-up:
	@echo "==> [1/6] Starting core infrastructure services..."
	$(COMPOSE) up -d postgres redis zookeeper kafka seaweedfs-master seaweedfs-volume seaweedfs-filer seaweedfs-s3
	@echo "==> [2/6] Waiting for Postgres..."
	@$(COMPOSE) exec -T postgres sh -c 'until pg_isready -U mlops -d mlops >/dev/null 2>&1; do echo "postgres not ready yet..."; sleep 2; done'
	@echo "==> [3/6] Ensuring MLflow metadata database exists..."
	@$(MAKE) ensure-mlflow-db
	@echo "==> [4/6] Waiting for SeaweedFS S3 endpoint and creating bucket if needed..."
	$(COMPOSE) run --rm mlflow-server python scripts/wait_for_s3.py --timeout 120 --create-bucket
	@echo "==> [5/6] Running Alembic migrations..."
	$(COMPOSE) run --rm --no-deps fraud-api python -m alembic upgrade head
	@echo "==> [6/6] Starting application and monitoring services..."
	$(COMPOSE) up -d mlflow-server fraud-api kafka-consumer lake-consumer binance-producer pushgateway prometheus alertmanager grafana alert-webhook gradio-demo
	@echo "==> Service status:"
	$(COMPOSE) ps
	@echo "==> Follow logs with: $(COMPOSE) logs -f --tail=100"

infra-down:
	$(COMPOSE) down -v

# Clean and rebuild all images (use this if infra-up is stuck)
infra-rebuild: clean
	@echo "==> Rebuilding Docker images (verbose BuildKit logs enabled)..."
	DOCKER_BUILDKIT=1 docker compose -f $(COMPOSE_FILE) --progress=plain build
	$(MAKE) infra-up

# 1. Labelled bootstrap data (only needed when the lake has never been seeded)
seed:
	$(COMPOSE) run --rm trainer python scripts/ingestion_ctl.py seed --n-rows 3000

# 2. Velocity features → Feast offline store, and the online store for serving
#    The post-apply check fails loudly if `feast apply` registered nothing
#    (e.g. the definitions module was shadowed by another `features` module).
features:
	$(COMPOSE) run --rm features sh -c "python features/compute.py && cd feature_repo && feast apply && feast feature-views list | grep -q user_stats && feast materialize-incremental $$(date -u +%Y-%m-%dT%H:%M:%S)"

# 3. Validated, featured training set
data:
	$(COMPOSE) run --rm trainer dvc repro build_dataset

# 4. Train + register (the quality gate decides whether a version is registered)
train:
	$(COMPOSE) run --rm trainer dvc repro train

bootstrap-model:
	@echo "Bootstrapping a Production model from scratch..."
	@$(MAKE) seed
	@$(MAKE) features
	@$(MAKE) data
	@$(MAKE) train
	@RUN_ID=$$(python -c "import json;print(json.load(open('metrics.json'))['run_id'])"); \
	$(COMPOSE) run --rm trainer python scripts/model_ctl.py promote --run-id "$$RUN_ID" --stage Production
	@echo "Restarting kafka-consumer to pick up the Production model..."
	@$(COMPOSE) up -d kafka-consumer

ensure-model:
	@echo "Checking if a Production model exists in MLflow..."
	@if $(COMPOSE) run --rm trainer python scripts/model_ctl.py exists --stage Production >/dev/null 2>&1; then \
		echo "Production model found. Skipping bootstrap."; \
	else \
		echo "No Production model found. Running bootstrap-model once..."; \
		$(MAKE) bootstrap-model; \
	fi

test:
	$(PYTHON) -m pytest tests/unit tests/integration -v

serve:
	@echo "fraud-api is served by Docker at http://localhost:8000"
	@echo "Use 'uvicorn api.main:app --reload' to run it on the host instead."

monitor:
	@echo "Grafana → http://localhost:3000 (admin/admin)"
	@echo "MLflow  → http://localhost:5000"

drift:
	$(COMPOSE) run --rm drift python monitoring/run_drift.py

all: infra-up ensure-model
	@echo "Platform is up and model is ready (existing or freshly bootstrapped)."

# Clean project-specific Docker resources only
clean:
	@echo "Cleaning project-specific Docker resources..."
	$(COMPOSE) down -v 2>/dev/null || true
	docker images '*fraud*' -q | xargs -r docker rmi -f 2>/dev/null || true
	docker images '*mlflow*' -q | xargs -r docker rmi -f 2>/dev/null || true
	docker images '*gradio*' -q | xargs -r docker rmi -f 2>/dev/null || true
	@echo "Project containers, images, and volumes cleaned."

db-revision:
	$(PYTHON) -m alembic revision --autogenerate -m "init"

db-upgrade:
	@$(COMPOSE) exec -T postgres sh -c 'until pg_isready -U mlops -d mlops >/dev/null 2>&1; do echo "postgres not ready yet..."; sleep 2; done'
	@$(COMPOSE) run --rm --no-deps fraud-api python -m alembic upgrade head

logs:
	$(COMPOSE) logs -f --tail=100
