# MLOps Platform — Comprehensive Implementation Plan

> **Architecture:** 5-lane pipeline · Data Ingestion → Training & Validation → Real-time Serving → Monitoring & Drift → CI/CD & Retraining

> **Data Lake:** SeaweedFS (Kubernetes, S3-compatible) as the unified object store backing every lane

> **Data Source:** Live Binance WebSocket trade feed (public, no auth) → Kafka → SeaweedFS

> **Domain:** Real-time fraud detection on crypto exchange trades, schema-mapped to the fraud detection format

---

## 0. Quick Reference — Tool Matrix

| Lane | Tool | Role | Free? |
|------|------|------|-------|
| Data Ingestion | **Binance WebSocket API** | Real-time public trade feed (no API key) | ✅ |
| Data Ingestion | **`kafka/producer.py`** | WebSocket → Kafka bridge, schema mapper | ✅ |
| Data Ingestion | **Apache Kafka** | Message bus (`transactions_raw` topic) | ✅ |
| Data Ingestion | **`kafka/lake_consumer.py`** | Kafka → SeaweedFS batch writer | ✅ |
| Data Ingestion | **Great Expectations** | Inline batch validation before lake write | ✅ |
| Data Ingestion | **Apache Parquet / PyArrow** | Columnar at-rest format (Snappy) | ✅ |
| Data Ingestion | **Feast** | Feature store (offline + online) | ✅ |
| Data Ingestion | **Redis** | Online feature cache for Feast | ✅ |
| Training | **scikit-learn / XGBoost** | Model training | ✅ |
| Training | **MLflow** | Experiment tracking + artifact store | ✅ |
| Training | **Optuna** | Hyperparameter optimisation (TPE + pruning) | ✅ |
| Training | **pytest + deepchecks** | Behavioural model quality gates | ✅ |
| Training | **MLflow Model Registry** | Versioned model promotion | ✅ |
| Serving | **FastAPI** | REST API (`/predict`, `/ingest/status`) | ✅ |
| Serving | **`kafka/consumer.py`** | Scoring worker (group `prediction-workers`) | ✅ |
| Serving | **PostgreSQL** | Prediction log + feedback store | ✅ |
| Serving | **Shadow mode / A/B** | Champion + challenger routing | ✅ |
| Monitoring | **Evidently AI** | Data & model drift reports | ✅ |
| Monitoring | **Prometheus** | Metrics scraping | ✅ |
| Monitoring | **Grafana** | Dashboards + alerting | ✅ |
| Monitoring | **Alertmanager** | Alert routing → retrain trigger | ✅ |
| CI/CD | **GitHub Actions** | Pipeline on push + alert-triggered retrain | ✅ |
| CI/CD | **DVC** | Data versioning over SeaweedFS | ✅ |
| CI/CD | **Docker + docker-compose** | Containerisation (7 services) | ✅ |
| CI/CD | **Render / HuggingFace Spaces** | Zero-cost public deployment | ✅ |
| Data Lake | **SeaweedFS (k8s)** | S3-compatible object storage | ✅ |

---

## 1. Data Source — Binance Public Trade Feed

### 1.1 Why Binance WebSocket

The training data is no longer generated synthetically at training time. Instead, a long-running producer connects to the **Binance public aggregate-trade WebSocket stream** — a genuinely free, high-frequency, real-world financial feed that requires zero authentication and zero API keys.

- URL: `wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/...`
- Docs: [Binance WebSocket Streams — Aggregate Trade Streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)
- Frequency: typically 5–50 messages per second per symbol
- Message format: `{"stream": "btcusdt@aggTrade", "data": {...}}`

### 1.2 Raw Binance `aggTrade` Message Shape

```json
{
  "e": "aggTrade",
  "E": 1672515782136,
  "a": 26129,
  "s": "BTCUSDT",
  "p": "0.01633102",
  "q": "4.70443515",
  "f": 27781,
  "l": 27781,
  "T": 1672515782136,
  "m": true
}
```

### 1.3 Schema Mapping → Fraud Detection Format

Each raw Binance message is mapped deterministically to the fraud schema in `kafka/producer.py`:

| Binance field | Fraud schema field | Mapping logic |
|---|---|---|
| `data.a` (aggregate tradeId) | `transaction_id` | `"txn_{symbol}_{a}"` |
| `data.T` (trade time ms) | `timestamp` | `datetime.fromtimestamp(T/1000, UTC).isoformat()` |
| `data.s` (symbol) | `currency`, `country` | lookup table (`BTCUSDT→USD/US`, `BTCEUR→EUR/DE`, …) |
| `data.p × data.q` | `amount_usd` | `min(float(p) * float(q), 49999.99)` |
| `data.m` (is maker) | `device_type` | `"exchange_maker"` / `"exchange_taker"` |
| derived from `a` | `user_id`, `merchant_id` | `(a // 100) % 50000`, `(a // 10) % 5000` |
| derived from `a` | `merchant_cat` | index into `["crypto_exchange", "electronics", …]` |
| `data.a` | `ip_hash` | `md5("{symbol}:{a}".encode())[:8]` |

### 1.4 Resulting Fraud Schema Record

```json
{
  "transaction_id":   "txn_btcusdt_26129",
  "timestamp":        "2023-01-01T03:09:42Z",
  "user_id":          "usr_00261",
  "merchant_id":      "mrch_2612",
  "merchant_cat":     "crypto_exchange",
  "amount_usd":       76.43,
  "currency":         "USD",
  "country":          "US",
  "device_type":      "exchange_maker",
  "ip_hash":          "d4e1f3a9",
  "card_last4":       "6129",
  "is_international": false,
  "hour_of_day":      3,
  "day_of_week":      6,
  "label":            0,
  "_source":          "binance_ws",
  "_symbol":          "BTCUSDT",
  "_trade_id":        26129,
  "_price":           0.01633102,
  "_quantity":        4.70443515
}
```

`label` is always `0` from the live feed — real fraud labels come from downstream labelling jobs or human review. The `_` prefixed provenance fields are stripped before Parquet write. Class imbalance for model training is handled via `scale_pos_weight` in XGBoost and the optional one-time seed command.

### 1.5 API-Friendly Design Decisions

- All identifiers use prefixed strings (`txn_`, `usr_`, `mrch_`) → easy regex filtering
- All timestamps are ISO-8601 UTC → no timezone conversion bugs
- `amount_usd` is capped at 49 999.99 to stay within GX bounds regardless of BTC price
- Provenance fields (`_source`, `_symbol`, etc.) are always stripped before Parquet write and model inference
- `label` is always the last column → training scripts use column-position slicing without hardcoding names

---

## 2. Data Lake — SeaweedFS on Kubernetes

### 2.1 Why SeaweedFS

- S3-compatible REST API — boto3, DVC, MLflow, s3fs all work out of the box with zero code changes
- Single binary per role: master, volume, filer, S3 gateway
- Horizontal volume scaling, erasure coding for redundancy
- Completely free and self-hosted — no egress fees

### 2.2 Bucket / Prefix Structure

```
s3://datalake/
├── raw/                          # Validated Parquet batches from lake_consumer.py
│   └── realtime_txn_YYYYMMDDTHHMMSSZ.parquet
├── quarantine/                   # Batches that failed GX validation
│   └── realtime_txn_YYYYMMDDTHHMMSSZ.parquet
├── features/                     # Feast offline store (partitioned by entity)
│   ├── entity=user/
│   └── entity=merchant/
├── models/                       # MLflow artifact root
│   └── fraud_detector/
│       └── v{N}/
│           ├── model.pkl
│           └── metadata.json
├── predictions/                  # Daily prediction log dumps
│   └── YYYY/MM/DD/preds.parquet
├── drift_reports/                # Evidently HTML + JSON reports
│   └── YYYY/MM/DD/report.json
└── dvc-cache/                    # DVC content-addressed cache
```

> **Note:** The `validated/` prefix from the original design is replaced by `raw/` (files are validated inline by `lake_consumer.py` before write) and `quarantine/` (failed batches). This removes a redundant copy step.

### 2.3 Kubernetes Deployment (Helm)

- Helm chart: `seaweedfs/seaweedfs` (community chart)
- 1× master pod (HA optional with Raft), 3× volume pods, 1× filer pod, 1× S3 gateway pod
- PersistentVolumeClaims: 50 Gi per volume pod (local-path or NFS)
- S3 gateway exposed as `ClusterIP` service `seaweedfs-s3:8333`
- Credentials stored in Kubernetes Secret → mounted as env vars in all pods

### 2.4 DVC Configuration

```ini
# .dvc/config
[core]
    remote = seaweedfs
[remote "seaweedfs"]
    url = s3://datalake/dvc-cache
    endpointurl = http://seaweedfs-s3:8333
    access_key_id = ${SEAWEED_ACCESS_KEY}
    secret_access_key = ${SEAWEED_SECRET_KEY}
```

---

## 3. Lane 1 — Data Ingestion (Streaming-Native)

The ingestion lane is now a fully streaming pipeline. No step generates a batch of data on demand — the lake fills continuously and autonomously from live market activity.

```
Binance WebSocket  ──►  kafka/producer.py  ──►  Kafka: transactions_raw
                                                        │
                                          ┌─────────────┴──────────────┐
                                          ▼                            ▼
                              kafka/lake_consumer.py       kafka/consumer.py
                              (group: lake-writers)        (group: prediction-workers)
                                          │                            │
                                          ▼                            ▼
                              SeaweedFS: raw/*.parquet        PostgreSQL: predictions
```

### 3.1 Component A — Binance WebSocket Producer (`kafka/producer.py`)

**Role:** Long-running process. Replaces `scripts/ingestion_ctl.py` as the source of training data.

- Connects to `wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/bnbusdt@aggTrade/solusdt@aggTrade/xrpusdt@aggTrade`
- Subscribes to up to 5 symbols simultaneously via the Binance combined stream endpoint
- Maps each `aggTrade` message to the fraud schema (see Section 1.3)
- Publishes one JSON record per trade to Kafka topic `transactions_raw`
- Handles WebSocket reconnection automatically via `websockets.connect()` async iterator
- Compression: `gzip` on the Kafka producer for wire efficiency
- Configurable via env: `BINANCE_WS_BASE`, `BINANCE_SYMBOLS`
- CLI flags: `--symbols`, `--dry-run` (print without publishing), `--max-messages` (for testing)

### 3.2 Component B — Kafka Topic `transactions_raw`

**Role:** Decouples the live feed from all downstream consumers.

- Topic: `transactions_raw` (partitions=6, replication=1 local / 3 production)
- Two independent consumer groups read this topic simultaneously:
  - `lake-writers` → `kafka/lake_consumer.py` (builds the training data lake)
  - `prediction-workers` → `kafka/consumer.py` (scores records in real time)
- Both groups advance their offsets independently — no message is lost or double-processed within either group

### 3.3 Component C — Streaming-to-Lake Consumer (`kafka/lake_consumer.py`)

**Role:** Builds the training data lake. New file, core of the new architecture.

Flush triggers (whichever fires first):
- `LAKE_BATCH_SIZE` messages accumulated (default: 1 000)
- `LAKE_BATCH_TIMEOUT_SECS` elapsed with at least one message (default: 300 s)

On each flush:
1. Strip provenance fields (`_source`, `_symbol`, `_trade_id`, `_price`, `_quantity`)
2. Coerce types (float, int, bool)
3. Run inline GX-equivalent validation:
   - No nulls in `transaction_id`, `amount_usd`, `label`, `timestamp`
   - `amount_usd` ∈ [0.01, 50 000]
   - `transaction_id` uniqueness ≥ 99 %
   - `label` ∈ {0, 1}
   - `hour_of_day` ∈ [0, 23]
4. **Pass** → write Snappy-compressed Parquet to `s3://datalake/raw/realtime_txn_<ISO_TS>.parquet`
5. **Fail** → write to `s3://datalake/quarantine/<ISO_TS>.parquet` and log the failed checks
6. Commit Kafka offset only after successful write

Consumer group: `lake-writers` (independent from `prediction-workers`)

### 3.4 `scripts/ingestion_ctl.py` — Repurposed as Control Utility

This script no longer generates training data. Its three sub-commands are:

| Sub-command | Purpose |
|---|---|
| `status` | Show lake file count, total size MB, quarantine count, Kafka config |
| `start` | Launch `kafka/producer.py` as a subprocess (local dev helper) |
| `seed` | **One-time bootstrap only** — write synthetic Parquet to lake when starting with zero historical data. Clearly labelled as exceptional; not used in the normal pipeline. |

### 3.5 `scripts/pipeline_gate.sh` — Repurposed as CI Readiness Gate

No longer runs data generation. Used by GitHub Actions as a pre-training gate:

1. Call `python scripts/ingestion_ctl.py status` → check `lake_raw_files ≥ MIN_RAW_FILES`
2. If lake is empty and `SEED_IF_EMPTY=true` → run `ingestion_ctl.py seed` (first-run bootstrap only)
3. If lake is empty and `SEED_IF_EMPTY=false` → exit non-zero (error: streaming pipeline not running)
4. Validate the most recent Parquet file with `python gx/validate.py`
5. Run `feast materialize-incremental` to refresh the online feature store

### 3.6 Great Expectations Suite (`gx/`)

The authoritative GX suite (`gx/expectations/fraud_transactions_suite.json`) is unchanged and is still used in CI via `gx/validate.py`. The inline validation in `lake_consumer.py` mirrors these rules for speed in the hot path, but is not a replacement for the full suite.

Expectations:
- `expect_column_values_to_not_be_null` — `transaction_id`, `amount_usd`, `label`, `timestamp`
- `expect_column_values_to_be_between` — `amount_usd` ∈ [0.01, 50 000]
- `expect_column_values_to_be_in_set` — `device_type`, `label`
- `expect_column_proportion_of_unique_values_to_be_between` — `transaction_id` ≥ 0.99
- `expect_column_mean_to_be_between` — `label` ∈ [0.005, 0.10]
- `expect_table_row_count_to_be_between` — [100, 10 000 000]

On pass → file is already in `raw/` (written by lake_consumer)
On fail → file goes to `quarantine/`; CI gate fails and blocks training

### 3.7 Feast Feature Store (`feast/`)

- **Feature repo:** `feature_store.yaml` pointing to SeaweedFS offline store
- **Entities:** `user_id`, `merchant_id`
- **Feature views:**
  - `user_features` — 7d rolling: avg_amount, txn_count, unique_merchants, fraud_rate_7d
  - `merchant_features` — 7d rolling: avg_amount, unique_users, chargeback_rate
- **Online store:** Redis (for sub-millisecond serving)
- `feast apply` on each validated dataset run
- `feast materialize-incremental` called from GitHub Actions after validation

### 3.8 Implementation Checklist

- [ ] `pip install websockets kafka-python pyarrow boto3 great_expectations feast redis`
- [ ] Deploy Binance producer as Docker service `binance-producer` with `restart: always`
- [ ] Deploy lake consumer as Docker service `lake-consumer` with `restart: always`
- [ ] Verify `transactions_raw` topic has 6 partitions
- [ ] Confirm two consumer groups exist independently (`lake-writers`, `prediction-workers`)
- [ ] Confirm Parquet files appear in `s3://datalake/raw/` within `LAKE_BATCH_TIMEOUT_SECS`
- [ ] Confirm quarantine path works by injecting a bad record
- [ ] Bootstrap GX datasource pointing to SeaweedFS via boto3
- [ ] Write Feast `feature_store.yaml`, entity definitions, feature views
- [ ] Unit tests: `tests/unit/test_producer_mapping.py`, `tests/unit/test_lake_consumer.py`
- [ ] Integration test: `tests/integration/test_ingestion_e2e.py`

---

## 4. Lane 2 — Training & Validation

Training is decoupled from data generation entirely. The training job simply reads whatever Parquet files have accumulated in `s3://datalake/raw/` and trains on them.

### 4.1 Components & Responsibilities

#### 4.1.1 `training/train.py`

- Load raw Parquet from `s3://datalake/raw/` via `s3fs`
- Pull offline features from Feast (`get_historical_features`)
- Merge transaction base with engineered features from `training/feature_engineering.py`
- Split: 80/20 stratified on `label`
- Train pipeline: `StandardScaler → XGBClassifier` (with `scale_pos_weight` for class imbalance)
- Hyperparameter search: `Optuna` (50 trials, TPE sampler, MedianPruner)
- Log every trial to MLflow: params, metrics (AUC-ROC, F1, avg precision)
- Register best model in MLflow Model Registry as `fraud_detector` version N
- Output `run_id` to stdout on last line for GitHub Actions capture

#### 4.1.2 MLflow (`mlflow/`)

- Tracking server: PostgreSQL backend (metrics) + SeaweedFS (artifacts via `MLFLOW_S3_ENDPOINT_URL`)
- Deployed as Kubernetes Deployment `mlflow-server`
- UI at port 5000, exposed via NodePort or Ingress
- Artifact root: `s3://datalake/models/`

#### 4.1.3 Model Tests (`tests/model/`)

Using **deepchecks** + custom pytest fixtures:

- `test_auc_above_threshold` — AUC-ROC ≥ 0.92 on validation set
- `test_f1_above_threshold` — F1 ≥ 0.70
- `test_no_feature_leakage` — mutual info between features and `transaction_id` / `timestamp` < 0.01
- `test_performance_by_segment` — AUC per `merchant_cat` and `device_type` all ≥ 0.85
- `test_prediction_confidence_distribution` — mean predicted prob not degenerate (0.001–0.5)
- `test_inference_latency_p99` — single-row predict under 50 ms

#### 4.1.4 MLflow Model Registry Promotion

Stages: `Staging` → `Production` → `Archived`

- `train.yml`: auto-promote to `Staging` if all model tests pass
- `retrain.yml`: auto-promote directly to `Production` (degraded model already confirmed by alert)
- Manual approval gate via GitHub Environment before `Production` promotion in `train.yml`
- `scripts/model_ctl.py` — thin wrapper around `MlflowClient.transition_model_version_stage`

### 4.2 Implementation Checklist

- [ ] `pip install mlflow xgboost optuna deepchecks s3fs scikit-learn`
- [ ] Configure `MLFLOW_TRACKING_URI` + `MLFLOW_S3_ENDPOINT_URL` in env
- [ ] Write `training/train.py` with Optuna + MLflow integration
- [ ] Write `training/feature_engineering.py` (Feast retrieval + stateless row-level transforms)
- [ ] Write all model tests in `tests/model/`
- [ ] Add `scripts/model_ctl.py`
- [ ] GitHub Actions workflow `train.yml` calling the lake-readiness gate → train → test → promote

---

## 5. Lane 3 — Real-time Serving

### 5.1 Components & Responsibilities

#### 5.1.1 FastAPI Application (`api/`)

Endpoints:

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/predict` | Synchronous single-record prediction |
| `POST` | `/predict/batch` | Batch prediction (≤ 1 000 rows) |
| `GET`  | `/ingest/status` | Lake file count + Kafka lag |
| `GET`  | `/health` | Liveness + readiness |
| `GET`  | `/metrics` | Prometheus exposition |
| `GET`  | `/model/info` | Current model version + metadata |

> **Note:** Ingestion is driven by the live Binance feed, not by API calls. `GET /ingest/status` exposes the same information that `ingestion_ctl.py status` returns, for monitoring dashboards.

Request body for `/predict`:
```json
{"transaction_id": "txn_btcusdt_26129", "amount_usd": 76.43, "merchant_cat": "crypto_exchange", ...}
```
Response:
```json
{"transaction_id": "txn_btcusdt_26129", "fraud_probability": 0.032, "decision": "APPROVE", "model_version": "3", "latency_ms": 4.1}
```

#### 5.1.2 Kafka Consumer for Predictions (`kafka/consumer.py`)

- Reads from `transactions_raw` (same topic as `lake_consumer.py`)
- Consumer group: `prediction-workers` (independent offset from `lake-writers`)
- Per message: deserialise → strip provenance fields → `engineer_features` → `model.predict_proba` → insert into `predictions` table
- `restart: always` in Docker / Kubernetes

#### 5.1.3 Prediction Log (PostgreSQL)

The prediction log stores one record per scored transaction and includes the following fields (managed via SQLAlchemy models in `db/models.py` and applied to the database using Alembic migrations):

- `id` — BIGSERIAL primary key
- `transaction_id` — TEXT, not null
- `model_version` — TEXT, not null
- `fraud_prob` — FLOAT, not null
- `decision` — TEXT, not null (values: `APPROVE` / `REJECT`)
- `features_json` — JSONB (optional feature snapshot)
- `shadow_prob` — FLOAT (optional challenger score)
- `latency_ms` — FLOAT (inference latency)
- `created_at` — TIMESTAMPTZ default NOW()

Indexes: add indexes on `created_at`, `model_version`, and `decision` to support time-series queries and model drill-downs.

Migration workflow: define models in `db/models.py`, create an autogenerated revision with `make db-revision` (Alembic autogenerate), review the generated file under `alembic/versions/`, then apply with `make db-upgrade`. `scripts/wait_for_postgres.py` is used to ensure Postgres is ready before migrations run.

#### 5.1.4 Shadow Mode / A/B Testing (`api/shadow.py`)

- `ShadowRouter`: every `/predict` request hits both `Production` and `Staging` models
- Champion response returned to client; challenger result stored in `shadow_prob`
- `GET /ab/report` → win-rate, AUC delta, sample counts per model
- Configurable via `SHADOW_TRAFFIC_FRACTION` env var (default 1.0 = full shadow)

### 5.2 Implementation Checklist

- [ ] `pip install fastapi uvicorn kafka-python prometheus-client psycopg2-binary websockets`
- [ ] Write `api/main.py`, `api/routers/predict.py`
- [ ] Add `api/routers/ingest_status.py` endpoint wiring
- [ ] Write `api/model_loader.py` — MLflow registry loader with in-process cache
- [ ] Write `kafka/consumer.py` reading from `transactions_raw`
- [ ] Write `api/shadow.py` A/B router
- [ ] Alembic migrations in `alembic/` with SQLAlchemy models in `db/models.py` — create revisions with `make db-revision` and apply with `make db-upgrade` (CI runs `make db-upgrade` after infra-up)
- [ ] Unit tests: `tests/unit/test_api.py` (TestClient), `tests/unit/test_shadow.py`
- [ ] Load test: `locust -f tests/load/locustfile.py` targeting `/predict`

---

## 6. Lane 4 — Monitoring & Drift

### 6.1 Components & Responsibilities

#### 6.1.1 Evidently AI (`monitoring/run_drift.py`)

Daily batch job (Kubernetes CronJob at 02:00 UTC):
- Pull reference window (sample of `raw/` Parquet files) from SeaweedFS
- Pull current window (yesterday's prediction log) from PostgreSQL
- Generate `DataDriftReport` + `ClassificationPerformanceReport`
- Save HTML + JSON to `s3://datalake/drift_reports/YYYY/MM/DD/`

Drift metrics surfaced:
- Dataset drift score (Jensen-Shannon divergence per feature)
- Share of drifted features
- PSI for `amount_usd`, `hour_of_day`
- Model performance: AUC, precision, recall vs baseline

Additional lake health metrics (from `lake_consumer.py` logs):
- Quarantine rate (failed GX batches / total batches)
- Batch flush frequency
- Messages per second by symbol

#### 6.1.2 Prometheus (`monitoring/prometheus/`)

FastAPI `/metrics` endpoint exposes:
- `predictions_total{decision, model_version}` counter
- `prediction_latency_seconds` histogram (p50, p95, p99)
- `fraud_probability_last` gauge
- `kafka_consumer_lag` gauge (both consumer groups)
- `lake_batches_total{status="pass|fail"}` counter (from lake_consumer)
- `lake_quarantine_rate` gauge

Prometheus scrape config: `scrape_interval: 15s`

#### 6.1.3 Grafana (`monitoring/grafana/`)

Dashboard: **MLOps Overview**

- Panel: Predictions/sec + error rate
- Panel: p99 latency time series
- Panel: Fraud detection rate
- Panel: Feature drift score (Evidently JSON → Prometheus pushgateway)
- Panel: Model AUC over time
- Panel: Kafka consumer lag (both `lake-writers` and `prediction-workers`)
- Panel: Lake batch pass/fail rate (new — shows GX inline validation health)
- Panel: Quarantine file count over time (new)

Provisioned via `monitoring/grafana/dashboards/overview.json`

#### 6.1.4 Alert → Retrain Trigger

- `FraudDriftHigh`: fires when `evidently_drift_score > 0.15` for 1 h
- `ModelAUCLow`: fires when `model_auc_roc < 0.90` for 30 min
- `HighQuarantineRate`: fires when `lake_quarantine_rate > 0.10` for 30 min (new — feed quality alert)
- `HighPredictionLatency`: fires when p99 latency > 100 ms for 5 min

All critical alerts → `monitoring/alert_webhook.py` → GitHub Actions `workflow_dispatch` on `retrain.yml`

### 6.2 Implementation Checklist

- [ ] `pip install evidently prometheus-client`
- [ ] Write `monitoring/run_drift.py`
- [ ] Add `lake_batches_total` and `lake_quarantine_rate` metrics to `lake_consumer.py`
- [ ] Add `kafka_consumer_lag` metric for both consumer groups
- [ ] Configure `prometheus.yml` scrape jobs
- [ ] Import Grafana dashboard JSON via provisioning (add two new lake panels)
- [ ] Write `monitoring/alert_webhook.py`
- [ ] Configure `alertmanager.yml` with `HighQuarantineRate` rule
- [ ] Kubernetes CronJob for daily drift run

---

## 7. Lane 5 — CI/CD & Retraining

### 7.1 Components & Responsibilities

#### 7.1.1 GitHub Actions Workflows (`.github/workflows/`)

| Workflow | Trigger | Steps |
|----------|---------|-------|
| `ci.yml` | Push to any branch | lint (ruff), unit tests, docker build, pip-audit |
| `train.yml` | Push to `main` or `workflow_dispatch` | **lake-readiness gate** → feast materialise → train → model tests → promote to Staging |
| `retrain.yml` | `workflow_dispatch` from alert webhook | lake-readiness gate (`SEED_IF_EMPTY=false`) → train → model tests → **promote directly to Production** |
| `deploy.yml` | On Staging→Production registry transition | docker push → Render/HF deploy |

> `train.yml` has a `lake-readiness` job that calls `scripts/pipeline_gate.sh` purely as a gate — verifying the lake is populated and refreshing Feast. The actual lake is filled continuously by `binance-producer` + `lake-consumer` running as persistent services.

> `retrain.yml` calls the script with `SEED_IF_EMPTY=false` — if the lake is empty during a retrain, that is an infrastructure error, not a recoverable condition.

#### 7.1.2 DVC (`dvc.yaml` + `params.yaml`)

- Tracks: `data/raw/` (symlinks to SeaweedFS via s3fs mount), `data/features/`
- Remote: SeaweedFS S3 bucket (`s3://datalake/dvc-cache`)
- `dvc repro` DAG: `validate → featurise → train` (generate stage removed)
- `params.yaml`: model hyperparameters + lake batch config (single source of truth)
- `dvc metrics show` / `dvc plots show` integrated into GitHub Actions PR comments

#### 7.1.3 Docker Services (`docker/docker-compose.yml`)

Full local stack — 13 services:

| Service | Image / Build | Role |
|---|---|---|
| `seaweedfs-master` | `chrislusf/seaweedfs:3.65` | SeaweedFS master |
| `seaweedfs-volume` | `chrislusf/seaweedfs:3.65` | SeaweedFS volume node |
| `seaweedfs-filer` | `chrislusf/seaweedfs:3.65` | SeaweedFS filer |
| `seaweedfs-s3` | `chrislusf/seaweedfs:3.65` | S3 gateway (port 8333) |
| `postgres` | `postgres:16-alpine` | Metrics + predictions store |
| `redis` | `redis:7-alpine` | Feast online store |
| `zookeeper` | `confluentinc/cp-zookeeper:7.6.1` | Kafka dependency |
| `kafka` | `confluentinc/cp-kafka:7.6.1` | Message bus |
| `binance-producer` | `Dockerfile.api` | WebSocket → Kafka (`restart: always`) |
| `lake-consumer` | `Dockerfile.api` | Kafka → SeaweedFS (`restart: always`) |
| `mlflow-server` | `Dockerfile.training` | Experiment tracker + model registry |
| `fraud-api` | `Dockerfile.api` | FastAPI serving |
| `kafka-consumer` | `Dockerfile.api` | Prediction worker (`restart: always`) |
| `prometheus` | `prom/prometheus:v2.52.0` | Metrics scraping |
| `alertmanager` | `prom/alertmanager:v0.27.0` | Alert routing |
| `grafana` | `grafana/grafana:10.4.2` | Dashboards |
| `alert-webhook` | `Dockerfile.monitoring` | GitHub Actions dispatch receiver |

#### 7.1.4 Deployment (Render / HuggingFace Spaces)

- `render.yaml` service definition — zero-cost web service tier
- `binance-producer` and `lake-consumer` deployed as Render background workers
- HuggingFace Space `app.py` — Gradio UI wrapping `/predict`
- Health check: `GET /health` every 30 s

### 7.2 Implementation Checklist

- [ ] Write all four GitHub Actions workflows (updated `train.yml`, `retrain.yml`)
- [ ] Remove `generate` stage from `dvc.yaml`; update DAG to `validate → featurise → train`
- [ ] Update `dvc params.yaml` with `lake_batch_size`, `lake_batch_timeout_secs`
- [ ] Add `binance-producer` and `lake-consumer` to `docker-compose.yml`
- [ ] Add `restart: always` to all three Kafka-connected services
- [ ] Write `render.yaml` with background worker entries for producer + lake consumer
- [ ] Configure branch protection + required status checks on `main`

---

## 8. Cross-cutting Concerns

### 8.1 Configuration Management

- All config via environment variables; `python-dotenv` for local dev
- `.env.example` committed; `.env` gitignored
- Kubernetes ConfigMaps for non-secret config, Secrets for credentials
- `config/settings.py` — Pydantic `BaseSettings` class (validated on startup)

### 8.2 Logging

- Structured JSON logging via `structlog`
- Log levels: DEBUG (dev), INFO (staging), WARNING (prod)
- Log fields: `timestamp`, `service`, `trace_id`, `transaction_id`, `model_version`
- `lake_consumer.py` logs: `batch_size`, `validation_status`, `s3_path`, `elapsed_s`
- Kubernetes: logs collected by Loki, visualised in Grafana

### 8.3 Testing Strategy

| Level | Framework | Location | When |
|-------|-----------|----------|------|
| Unit | pytest | `tests/unit/` | Every push |
| Integration | pytest | `tests/integration/` | Every push to main |
| Model | deepchecks + pytest | `tests/model/` | After training |
| Load | Locust | `tests/load/` | Pre-deploy |
| Contract | schemathesis | `tests/contract/` | Every push |

### 8.4 Security Baseline

- SeaweedFS and Binance-related env vars in Kubernetes Secrets (never in code)
- Binance WebSocket is public (no API key) — no secret required for ingestion
- FastAPI CORS restricted to allowed origins
- JWT authentication on all non-health endpoints (toggled by env)
- Dependency scanning via `pip-audit` in CI
- Docker image scanning via Trivy in CI

---

## 9. Local Development Quickstart (implemented in `Makefile`)

```makefile
make setup        # pip install -r requirements-dev.txt + pre-commit install
make infra-up     # docker-compose up -d (all 17 services including producer + lake consumer)
make status       # python scripts/ingestion_ctl.py status  (check lake file count)
make seed         # python scripts/ingestion_ctl.py seed    (one-time bootstrap only)
make train        # python training/train.py
make test         # pytest tests/unit tests/integration
make model-test   # pytest tests/model
make serve        # uvicorn api.main:app --reload
make monitor      # Grafana → http://localhost:3000 | MLflow → http://localhost:5000
make drift        # python monitoring/run_drift.py
make all          # infra-up → (wait for lake to fill) → train → serve
```

> **First-run workflow:**
> 1. `make infra-up` — starts all services including `binance-producer` and `lake-consumer`
> 2. Wait ~5 minutes for the first Parquet batch to land in SeaweedFS, OR run `make seed`
> 3. `make train` — trains on whatever is in the lake
> 4. `make serve` — API live at `http://localhost:8000`

---

## 10. Milestone Roadmap

| Week | Milestone | Deliverable |
|------|-----------|-------------|
| 1 | Streaming foundation | Binance producer running, `transactions_raw` topic live, lake consumer writing Parquet |
| 2 | Data lake + GX | SeaweedFS populated, GX inline validation confirmed, quarantine path tested |
| 3 | Feature store | Feast entities + views, Redis online store, `materialize-incremental` working |
| 4 | Training pipeline | `train.py` + MLflow + Optuna + model registry promotion |
| 5 | Model quality gates | deepchecks tests + `ci.yml` + `train.yml` lake-readiness gate |
| 6 | Serving API | FastAPI `/predict` + prediction Kafka consumer + PostgreSQL log |
| 7 | Shadow mode | A/B router + `shadow_prob` column + `/ab/report` endpoint |
| 8 | Monitoring | Evidently + Prometheus + Grafana (incl. lake health panels) |
| 9 | Alerts + auto-retrain | Alertmanager rules → webhook → `retrain.yml` dispatch |
| 10 | Deploy + demo | Render background workers + HuggingFace Gradio demo |
