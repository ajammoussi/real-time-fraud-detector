# Architecture

Five lanes, one data lake, one feature store. The diagram in the
[README](../README.md) shows how they connect; this page explains what each
piece owns and where the boundaries are.

## Lanes

| Lane | Owns | Processes |
|---|---|---|
| **Streaming** | Trade feed → lake | `ingestion/producer.py`, `ingestion/lake_consumer.py` |
| **Data lake + features** | At-rest data, velocity features | `datalake/storage.py`, `features/compute.py`, `feature_repo/` |
| **Training** | Dataset, gate, model registration | `training/build_dataset.py`, `training/train.py`, `gx/` |
| **Serving** | Scoring, audit rows | `api/`, `ingestion/predict_consumer.py`, `db/` |
| **Observability + CI/CD** | Drift, alerts, retraining | `monitoring/`, `.github/workflows/` |

## Components

### Ingestion & validation
- **Binance producer** (`ingestion/producer.py`) — maps `aggTrade` messages to
  the transaction schema and publishes to `transactions_raw`. Records are
  deliberately unlabelled; `country`/`is_international` are derived from the
  quote asset, and `country` is never a model feature.
- **Lake consumer** (`ingestion/lake_consumer.py`) — batches the topic into
  Parquet under `s3://datalake/raw/`, diverts failing batches to `quarantine/`,
  and commits offsets only after a durable write.
- **Great Expectations** (`gx/validate.py`, `gx/expectations/`) — the training
  dataset contract, enforced as a hard gate inside `training/build_dataset.py`.

### Features
- **Feast repository** (`feature_repo/definitions.py`) — `user_stats` and
  `merchant_stats` feature views over Parquet in the lake, materialised into
  Redis for online reads.
- **Computation** (`features/compute.py`) — hourly aggregates over a trailing
  window, written to `s3://datalake/features/`.
- **Access layer** (`features/store.py`) — the only module that touches Feast.

### Training
- **Feature contract** (`training/feature_engineering.py`) — deterministic
  row-level features and the ordered `FEATURE_COLS` shared with serving.
- **Dataset build** (`training/build_dataset.py`) — lake → sample → GX gate →
  Feast point-in-time join → `data/dataset.parquet`.
- **Training** (`training/train.py`) — 60/20/20 split, Optuna search, PR-AUC
  early stopping, F1-maximising threshold, quality gate **before** registration.
- **Registry** — MLflow. Training never promotes; `scripts/model_ctl.py promote`
  is the only path into Staging/Production.

### Serving
- **FastAPI app** (`api/`) — `POST /predict/`, `GET /predict/model/info`,
  `GET /ingest/status`, `GET /health`, `GET /metrics`.
- **Prediction consumer** (`ingestion/predict_consumer.py`) — the same scoring
  path over Kafka, on its own consumer group.
- **Model loader** (`api/model_loader.py`) — artifact **and** registry metadata
  cached together, refreshed on a TTL, so the version stamped on a prediction
  always matches the model that produced it.
- **Shadow routing** (`api/shadow.py`) — champion/challenger sampling with a
  TTL-guarded challenger lookup.
- **Audit** (`db/writer.py`) — idempotent inserts keyed by `transaction_id`.

### Observability
- **Drift job** (`monitoring/run_drift.py`) — Evidently over two windows of
  served predictions, reports uploaded to the lake, share pushed to Prometheus.
- **Alerting** — Prometheus rules, Alertmanager routing, and an authenticated
  webhook (`monitoring/alert_webhook.py`) that dispatches the retraining
  workflow.

## Tech stack

| Area | Tools |
|---|---|
| Model | XGBoost (CPU), Optuna, scikit-learn |
| Features | Feast, Redis, pandas, PyArrow |
| Track / registry | MLflow |
| Validation | Great Expectations |
| Drift | Evidently |
| Data / pipeline | SeaweedFS (S3), Parquet, DVC |
| Streaming | Apache Kafka |
| Serving | FastAPI, Uvicorn, Pydantic |
| Audit | PostgreSQL 16 + SQLAlchemy 2.0 + Alembic |
| Monitoring | Prometheus, Grafana, Alertmanager |
| Packaging | Docker Compose, Kubernetes manifests |
| CI/CD | GitHub Actions, Render |
| Quality | pytest, Locust, ruff, pre-commit |

## Data contracts

### The model vector
`training/feature_engineering.FEATURE_COLS` is the single definition, in order:

```
amount_usd, amount_log, is_high_amount, is_odd_hour, is_weekend,
is_international, hour_of_day, day_of_week, currency, merchant_cat, device_type,
user_txn_count_7d, user_amount_sum_7d, user_amount_avg_7d, user_amount_max_7d,
merchant_txn_count_7d, merchant_amount_avg_7d
```

`amount_usd`, `merchant_cat`, `device_type` and the velocity columns are
transformed in place, so the vector is numeric and 17 columns wide.

### The audit row
`predictions` stores `transaction_id`, `model_version`, `fraud_prob`,
`decision`, `features_json` (the exact scored vector), `shadow_prob`,
`latency_ms`, `created_at`. The unique index on `transaction_id` makes replays
no-ops.

## Repository layout

```
api/                 FastAPI app: routers, model loader, shared scoring, shadow, metrics
ingestion/           producer.py (Binance → Kafka), lake_consumer.py, predict_consumer.py
training/            build_dataset.py, train.py, feature_engineering.py
features/            compute.py (velocity), schema.py (names), store.py (Feast)
feature_repo/        definitions.py (feature views), feature_store.yaml, registry.db
datalake/            storage.py (S3 client, parquet IO, lake_status)
gx/                  validate.py, expectations/fraud_transactions_suite.json
monitoring/          run_drift.py, alert_webhook.py, prometheus/, grafana/, alertmanager/
db/  alembic/        models.py, writer.py, migrations
config/              settings.py (env), params.py (params.yaml)
docker/              compose stack + Dockerfile.{api,training,monitoring,gradio}
scripts/             ingestion_ctl.py, model_ctl.py, pipeline_gate.sh, wait_for_{s3,postgres}.py
tests/               unit/, integration/, load/
.github/workflows/   ci.yml, _train.yml, train.yml, retrain.yml, deploy.yml
params.yaml          Optuna search space + quality gates
dvc.yaml             build_dataset and train stages
```

Kubernetes manifests (`k8s/`) and the Render config (`render.yaml`) exist for
deployment; the local stack runs entirely from `docker/docker-compose.yml`.
