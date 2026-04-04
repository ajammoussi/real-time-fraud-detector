.PHONY: setup infra-up infra-down infra-rebuild data train test serve serve-local monitor drift all all-local clean bootstrap-model ensure-model ensure-mlflow-db logs

# Enable Docker BuildKit for faster caching (must be run in terminal or .env)
export DOCKER_BUILDKIT=1

COMPOSE_FILE=docker/docker-compose.yml
COMPOSE=docker compose -f $(COMPOSE_FILE)

setup:
	pip install -r requirements/dev.txt
	pre-commit install

ensure-mlflow-db:
	@echo "==> Ensuring MLflow metadata database exists (idempotent)..."
	@$(COMPOSE) exec -T postgres sh -ec '
	for i in $$(seq 1 20); do
	  if psql -U mlops -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='"'"'mlflow'"'"';" | grep -q 1; then
	    echo "mlflow database already exists.";
	    exit 0;
	  fi;
	  if psql -U mlops -d postgres -c "CREATE DATABASE mlflow;" >/dev/null 2>&1; then
	    echo "Created mlflow database.";
	    exit 0;
	  fi;
	  echo "mlflow database not ready yet, retry $$i/20...";
	  sleep 2;
	done;
	echo "Failed to ensure mlflow database after retries.";
	exit 1'

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
	$(COMPOSE) up -d mlflow-server fraud-api kafka-consumer lake-consumer binance-producer prometheus alertmanager grafana alert-webhook gradio-demo
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

data:
	bash scripts/pipeline_gate.sh

train:
	python training/train.py

bootstrap-model:
	@echo "Waiting for SeaweedFS S3 endpoint..."
	@$(COMPOSE) run --rm mlflow-server python scripts/wait_for_s3.py --timeout 120 --create-bucket
	@echo "Seeding labeled bootstrap data into lake..."
	@$(COMPOSE) run --rm mlflow-server python scripts/ingestion_ctl.py seed --n-rows 3000
	@echo "Training + registering model in MLflow..."
	@$(COMPOSE) run --rm mlflow-server python training/train.py
	@echo "Restarting kafka-consumer to load Production model..."
	@$(COMPOSE) up -d kafka-consumer

ensure-model:
	@echo "Checking if a Production model exists in MLflow..."
	@if $(COMPOSE) run --rm mlflow-server python scripts/model_ctl.py exists --stage Production >/dev/null 2>&1; then \
		echo "Production model found. Skipping bootstrap."; \
	else \
		echo "No Production model found. Running bootstrap-model once..."; \
		$(MAKE) bootstrap-model; \
	fi

test:
	pytest tests/unit tests/integration -v

model-test:
	pytest tests/model -v

serve:
	@echo "fraud-api is served by Docker at http://localhost:8000"
	@echo "Use 'make serve-local' to run uvicorn directly on the host."

serve-local:
	uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

monitor:
	@echo "Grafana → http://localhost:3000 (admin/admin)"
	@echo "MLflow  → http://localhost:5000"

drift:
	python monitoring/run_drift.py

all: infra-up ensure-model
	@echo "Platform is up and model is ready (existing or freshly bootstrapped)."

all-local: infra-up data train model-test serve-local

# Clean project-specific Docker resources only
clean:
	@echo "Cleaning project-specific Docker resources..."
	$(COMPOSE) down -v 2>/dev/null || true
	docker images '*fraud*' -q | xargs -r docker rmi -f 2>/dev/null || true
	docker images '*mlflow*' -q | xargs -r docker rmi -f 2>/dev/null || true
	docker images '*gradio*' -q | xargs -r docker rmi -f 2>/dev/null || true
	@echo "Project containers, images, and volumes cleaned."

db-revision:
	alembic revision --autogenerate -m "init"

db-upgrade:
	@$(COMPOSE) exec -T postgres sh -c 'until pg_isready -U mlops -d mlops >/dev/null 2>&1; do echo "postgres not ready yet..."; sleep 2; done'
	@$(COMPOSE) run --rm --no-deps fraud-api python -m alembic upgrade head

logs:
	$(COMPOSE) logs -f --tail=100
