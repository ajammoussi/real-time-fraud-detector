# Operations

## Prerequisites

- Docker 24+ with the Compose plugin (`docker compose`)
- Python 3.11+ (host tooling only — pipeline stages run in containers)
- 8 GB RAM minimum, 16 GB recommended; ~20 GB disk for images and lake data

The Binance feed is public; the only credentials you need locally are the
SeaweedFS keys that ship in `.env.example`. GitHub and Render accounts are only
needed for cloud deployment.

## Setup

```bash
git clone https://github.com/YOUR_ORG/real-time-fraud-detector.git
cd real-time-fraud-detector
cp .env.example .env
```

`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` must match the SeaweedFS keys in
the same file: MLflow and the API read model artifacts from `s3://datalake/…`
with them.

Optional host environment for tests and linters:

```bash
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements/dev.txt
pre-commit install
```

### GitHub Actions secrets

Under **Settings → Secrets and variables → Actions**:

| Kind | Name | Used by |
|---|---|---|
| Secret | `GITHUB_TOKEN` | alert webhook dispatching the retrain workflow |
| Secret | `SEAWEED_ACCESS_KEY`, `SEAWEED_SECRET_KEY`, `SEAWEED_ENDPOINT` | training pipeline's lake access |
| Variable | `AUTO_RETRAIN_ENABLED` | weekly scheduled retraining (see [Training](training.md#retraining-switch)) |
| Variable | `RENDER_DEPLOY` | Render deployment after a successful training run |

## Commands

| Target | What it does |
|---|---|
| `make setup` | install dev requirements, create `.env`, install hooks |
| `make infra-up` | core services, MLflow database, migrations, then apps + monitoring |
| `make infra-down` | stop everything and delete volumes |
| `make infra-rebuild` | clean, rebuild images, `infra-up` |
| `make seed` | write a labelled synthetic batch to the lake |
| `make features` | velocity features → lake → `feast apply` → materialise Redis |
| `make data` | `dvc repro build_dataset` |
| `make train` | `dvc repro train` (gate decides whether anything is registered) |
| `make bootstrap-model` | seed → features → data → train → promote, restart consumer |
| `make ensure-model` | promote path only if no Production model exists |
| `make all` | `infra-up` + `ensure-model` |
| `make drift` | run the drift job |
| `make test` | unit + integration tests (`PYTHON=` to point at another interpreter) |
| `make db-upgrade` / `db-revision` | apply / autogenerate Alembic migrations |
| `make logs` | follow all service logs |
| `make clean` | remove project containers, volumes and images |

## Running the pieces

```bash
# streaming
docker compose -f docker/docker-compose.yml up -d binance-producer lake-consumer
docker compose -f docker/docker-compose.yml logs -f lake-consumer

# training stages (one-shot jobs)
make seed && make features && make data && make train

# serving
docker compose -f docker/docker-compose.yml up -d fraud-api kafka-consumer

# demo
docker compose -f docker/docker-compose.yml up -d gradio-demo
```

The API can also run on the host against the containerised dependencies:

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

Set `MLFLOW_S3_ENDPOINT_URL=http://localhost:8333` in that shell, otherwise the
artifact download will try the container hostname.

## Verifying the stack

```bash
docker compose -f docker/docker-compose.yml ps                 # every service Up (mlflow + postgres healthy)
curl -s localhost:8000/health                                 # {"status":"ok"}
curl -s localhost:8000/predict/model/info                     # served version + decision threshold
curl -s localhost:8000/ingest/status                          # lake_raw_files > 0 once ingestion runs
PYTHONPATH=. SEAWEED_ENDPOINT=http://localhost:8333 \
  python scripts/ingestion_ctl.py status                      # same counts, as JSON
docker compose -f docker/docker-compose.yml exec kafka \
  kafka-topics --bootstrap-server localhost:9092 --list        # transactions_raw
curl -s localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job:.labels.job, health}'
```

## Configuration

`config/settings.py` reads these from the environment (see `.env.example`):

| Group | Keys |
|---|---|
| Lake | `SEAWEED_ENDPOINT`, `SEAWEED_ACCESS_KEY`, `SEAWEED_SECRET_KEY`, `DATALAKE_BUCKET` |
| MLflow | `MLFLOW_TRACKING_URI`, `MLFLOW_S3_ENDPOINT_URL`, `MLFLOW_EXPERIMENT_NAME` |
| Postgres | `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` |
| Kafka | `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC_TRANSACTIONS_RAW`, `KAFKA_CONSUMER_GROUP`, `KAFKA_CONSUMER_GROUP_LAKE` |
| Feed | `BINANCE_WS_BASE`, `BINANCE_SYMBOLS` |
| Lake batching | `LAKE_BATCH_SIZE`, `LAKE_BATCH_TIMEOUT_SECS` |
| Serving | `MODEL_NAME`, `MODEL_STAGE`, `CHALLENGER_STAGE`, `MODEL_CACHE_TTL_SECS`, `DECISION_THRESHOLD`, `SHADOW_TRAFFIC_FRACTION` |
| Features | `FEATURE_REPO_PATH`, `FEATURE_STATS_PREFIX` |
| Monitoring | `PUSHGATEWAY_URL`, `ALERT_WEBHOOK_TOKEN`, `CONSUMER_METRICS_PORT` |

Non-secret hyperparameters live in `params.yaml` and are read through
`config/params.py`.

## Troubleshooting

**A container keeps restarting.** Read its log first — the common causes are a
dependency that was not ready yet (Kafka before its listener, SeaweedFS S3
before the filer handshake) and a stale image. `make infra-rebuild` is the
blunt fix; `docker compose -f docker/docker-compose.yml ps` names the culprit.

**`mlflow-server` is unhealthy.** It needs the `mlflow` database (created by
`make ensure-mlflow-db`) and the S3 endpoint. The healthcheck is a TCP connect
to port 5000, so "unhealthy" usually means it crashed at startup — check the log
for a backend-store connection error.

**`feast apply` registers nothing.** A local module named `features.py` inside
`feature_repo/` would be shadowed by the project's `features/` package on
`PYTHONPATH`; the definitions live in `definitions.py` for that reason. The
Makefile target fails loudly if no view is registered.

**`make data` says the lake has no `label` column.** Only synthetic seed batches
carry labels. Run `make seed` (the loader reads `raw/seed_*`).

**Drift fails with "not enough predictions".** Both windows need ≥ 200 rows;
`predictions` fills up only while the consumer or API is scoring.

**`make test` collects nothing / import errors.** `pytest` from a different
interpreter is on `PATH`. Use `make test PYTHON=.venv/Scripts/python.exe`
(Windows) or `PYTHON=.venv/bin/python`.

**Prometheus says a target is down.** Targets are container DNS names on the
compose network; a service must be up and publish the scraped port (`8000`,
`8002`, `9101`). `curl localhost:9090/api/v1/targets` shows the last error.

**The drift alert never fires locally.** `FraudDriftHigh` waits 1 hour, and the
gauge only exists after a drift run has pushed it.

## Deployment

- `k8s/mlops/fraud-api-deployment.yaml` — autoscaled API deployment
- `k8s/mlops/drift-cronjob.yaml` — scheduled drift job
- `k8s/seaweedfs/` — Helm values and credentials for the lake
- `render.yaml` — zero-cost Render deployment used by `deploy.yml`
  (gated by `RENDER_DEPLOY`)

Images are built by `docker/Dockerfile.{api,training,monitoring,gradio}`;
the compose file builds the same four and reuses them across services.
