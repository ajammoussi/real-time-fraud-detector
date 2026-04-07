# 🚀 Real-Time Fraud Detection MLOps Platform

> **Enterprise-Grade MLOps Pipeline with Data Engineering, Model Training, Real-Time Serving, and Drift Monitoring**

A production-ready MLOps platform implementing a comprehensive 5-lane pipeline for fraud detection on e-commerce transactions and crypto trades. This project demonstrates end-to-end machine learning operations including data orchestration, feature engineering, model training with hyperparameter optimization, real-time serving, shadow deployment, data drift detection, and automated retraining—all running on containerized infrastructure.

## 📋 Table of Contents

- [🎯 Project Overview](#-project-overview)
- [🏗️ Architecture & Pipeline Lanes](#-architecture--pipeline-lanes)
- [📦 What's Included](#-whats-included)
- [🛠️ Tech Stack](#-tech-stack)
- [📋 Prerequisites](#-prerequisites)
- [⚙️ Configuration & Setup](#-configuration--setup)
- [🚀 Quick Start](#-quick-start)
- [📚 Running the Project](#-running-the-project)
- [📁 Project Structure](#-project-structure)
- [🧪 Testing](#-testing)
- [📊 Monitoring & Observability](#-monitoring--observability)
- [🏷️ Dataset & labels](#-dataset--labels)
- [🔁 Retraining switch](#-retraining-switch)

---

## 🎯 Project Overview

This MLOps platform demonstrates a **professional-grade production pipeline** for fraud detection featuring:

### Core Capabilities

- **Real-Time Data Ingestion**: Live Binance WebSocket trade feed (public API, no authentication required) streamed via Apache Kafka
- **Data Lake**: S3-compatible SeaweedFS for unified data storage across all pipeline stages
- **Feature Engineering**: Automated feature computation with Feast (offline + online store backed by Redis)
- **ML Training**: XGBoost with Optuna hyperparameter optimization, MLflow experiment tracking and model registry
- **Model Validation**: Great Expectations dataset contract, enforced inside the dataset build
- **Real-Time Serving**: FastAPI REST API with sub-100ms latency and shadow/canary deployment patterns
- **Prediction Logging**: PostgreSQL-backed audit trail for compliance and feedback loops
- **Data Drift Detection**: Evidently AI statistical drift reports with automated retraining triggers
- **Monitoring & Alerting**: Prometheus + Grafana dashboards with Alertmanager webhook integration
- **CI/CD Pipeline**: GitHub Actions for automated testing, training, and deployment on push/schedule
- **Data Versioning**: DVC integration for reproducible ML workflows

### Sample Domain

Fraud detection on crypto exchange trades (synthesized from the Binance public WebSocket feed) with features derived in real time:
- Transaction metadata: amount, quote currency, device type — plus `is_international`, derived from the quote asset rather than real geography
- Per-user and per-merchant velocity: 7-day transaction counts and amount sums/averages/maxima over a trailing window
- Time-based context: hour of day, weekday, odd-hour and weekend flags

Because the feed is unlabelled, the classifier is trained on the synthetic seed
labels — see [Dataset & labels](#-dataset--labels).

---

## 🏗️ Architecture & Pipeline Lanes

The platform implements a **5-lane streaming architecture** with SeaweedFS as the unified data lake:

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                      Platform Pipeline                                                │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────┐   ┌─────────────────────────┐   ┌─────────────────────────┐   ┌─────────────────────────┐   ┌─────────────────────────┐
│    LANE 1               │   │    LANE 2               │   │    LANE 3               │   │    LANE 4               │   │    LANE 5               │
│    DATA INGESTION       │   │    TRAINING &           │   │    REAL-TIME            │   │    MONITORING           │   │    CI/CD &              │
│                         │   │    VALIDATION           │   │    SERVING              │   │    & DRIFT              │   │    RETRAINING           │
├─────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤
│ Binance WebSocket       │   │ SeaweedFS               │   │ Kafka Prediction        │   │ PostgreSQL              │   │ GitHub Actions          │
│ (public feed)           │   │ (raw parquet)           │   │ Consumer                │   │ Predictions             │   │ (on push / scheduled)   │
├─────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤
│ Kafka Producer          │   │ Feature Engineering     │   │ FastAPI Prediction      │   │ Evidently AI            │   │ DVC Pipeline            │
│ (schema mapping)        │   │ (Feast)                 │   │ Service                 │   │ (drift detection)       │   │ (data versioning)       │
├─────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤
│ Kafka Topic:            │   │ MLflow                  │   │ Model Inference         │   │ Prometheus              │   │ Training Container      │
│ transactions_raw        │   │ (exp tracking, HPO)     │   │ (shadow routing)        │   │ Rules Engine            │   │ (early stopping)        │
├─────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤
│ Lake Consumer           │   │ Model Registry          │   │ PostgreSQL Audit Log    │   │ Alertmanager            │   │ Model Promotion         │
│ (GX validation)         │   │ (versioning, promotion) │   │                         │   │ (retrain triggers)      │   │ (Production stage)      │
├─────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤   ├─────────────────────────┤
│ SeaweedFS               │   │ MLflow Artifacts        │   │ Prometheus Metrics      │   │                         │   │ API Redeployment        │
│ (s3://datalake/raw/)    │   │ (s3://datalake/models/) │   │                         │   │                         │   │                         │
└─────────────────────────┘   └─────────────────────────┘   └─────────────────────────┘   └─────────────────────────┘   └─────────────────────────┘
```

---

## 📦 What's Included

### Data Ingestion & Validation
- **Binance WebSocket Producer** (`ingestion/producer.py`): real-time trade feed to Kafka. The record is deliberately unlabelled; `country`/`is_international` are derived from the quote asset (a crypto feed has no geography) and `country` is never a model feature
- **Lake Consumer** (`ingestion/lake_consumer.py`): Kafka → SeaweedFS batch writer with configurable flush windows, cheap pre-checks that divert bad batches to `quarantine/`, and offsets committed only after a durable write
- **Great Expectations** (`gx/validate.py`, `gx/expectations/`): the training dataset contract, enforced as a hard gate inside `training/build_dataset.py`
- **Feast Feature Store** (`feature_repo/definitions.py`, `features/compute.py`, `features/store.py`): hourly user/merchant velocity aggregates in the offline store, served online from Redis

### Model Training Pipeline
- **Feature Engineering** (`training/feature_engineering.py`): deterministic row-level features plus the ordered `FEATURE_COLS` contract shared with serving
- **Dataset Build** (`training/build_dataset.py`): lake → deterministic sample → GX contract → Feast point-in-time join → `data/dataset.parquet`
- **Training Script** (`training/train.py`): XGBoost + Optuna on a 60/20/20 split, with:
  - PR-AUC objective (the right metric at a ~2% base rate) and early stopping on validation
  - an F1-maximising decision threshold, picked on validation and stored as a model-version tag
  - a quality gate that runs **before** registration, so a failing model is never registered
- **Model Registry**: `scripts/model_ctl.py promote` is the only thing that moves a run into Staging/Production; training never promotes

### Real-Time Serving
- **FastAPI API** (`api/main.py`): REST endpoints
  - `POST /predict/` — one transaction, scored through the shared `api/inference.score` path
  - `GET /predict/model/info` — registry metadata for the served stage
  - `GET /ingest/status` — lake counts, read straight from the data lake
  - `GET /health`, `GET /metrics`
- **Prediction Consumer** (`ingestion/predict_consumer.py`): batch scoring worker on its own consumer group
- **Model Loader** (`api/model_loader.py`): registry-backed cache re-checked on a TTL, so a promoted model reaches the API without a restart
- **Shadow Routing** (`api/shadow.py`): champion/challenger sampling with a TTL-guarded challenger lookup
- **Prediction Audit** (`db/writer.py`): idempotent rows in `predictions`, storing the exact feature vector that was scored

### Monitoring & Observability
- **Prometheus** (`monitoring/prometheus/prometheus.yml`): scrapes the API, both consumers and the Pushgateway
- **Grafana** (`monitoring/grafana/dashboards/overview.json`): throughput, latency, fraud rate, drift share, lake writes, consumer lag
- **Evidently AI** (`monitoring/run_drift.py`): drift between two windows of *served* predictions, published as `evidently_drift_share`
- **Alertmanager + Alert Webhook** (`monitoring/alert_webhook.py`): firing drift alerts dispatch the GitHub retrain workflow (bearer-token authenticated)

### Testing & Quality Assurance
- **Unit Tests** (`tests/unit/`): API, feature contract, GX suite, dataset build, scorer, lake status
- **Integration Test** (`tests/integration/`): offline round-trip producer → lake pre-checks → model input
- **Load Testing** (`tests/load/locustfile.py`): Locust load simulation

### CI/CD & Orchestration
- **GitHub Actions** (`.github/workflows/`):
  - `ci.yml` — pre-commit (ruff lint + format), unit tests with coverage, image import check
  - `_train.yml` — the single training pipeline, called by the two triggers below
  - `train.yml` — push-triggered training into `Staging`
  - `retrain.yml` — weekly + manual/webhook retraining into `Production`, gated by `AUTO_RETRAIN_ENABLED`
  - `deploy.yml` — Render deploy after a successful training run, gated by `RENDER_DEPLOY`
- **DVC Pipeline** (`dvc.yaml`): two stages, `build_dataset` and `train`
- **Makefile**: one-command orchestration; every pipeline stage is a compose job
- **Docker Compose**: full local stack — 22 services, 3 of them one-shot jobs behind the `jobs` profile

### Database Migrations
- **Alembic** (`alembic/versions/`): schema versioning, including the unique index that makes prediction writes idempotent

### Kubernetes Deployment
- **Fraud API Deployment** (`k8s/mlops/fraud-api-deployment.yaml`): Autoscaled prediction service
- **Drift CronJob** (`k8s/mlops/drift-cronjob.yaml`): Scheduled drift detection
- **SeaweedFS Helm Values** (`k8s/seaweedfs/values.yaml`): Data lake configuration
- **RBAC & Secrets** (`k8s/seaweedfs/secret.yaml`): Credentials management

### UI & Demos
- **Gradio App** (`app.py`): Interactive web demo for single predictions
- **Render Deployment** (`render.yaml`): Zero-cost public deployment configuration

---

## 🛠️ Tech Stack

### Core ML/Data
| Component | Tool | Version | Purpose |
|-----------|------|---------|---------|
| Model Training | XGBoost | >=2.0 | Gradient boosting for fraud classification |
| Hyperparameter Optimization | Optuna | >=3.6 | TPE search with early stopping on validation |
| Feature Store | Feast | >=0.61 | Offline + online feature registry |
| Feature Computation | pandas + PyArrow | 2.2+ / 21+ | Columnar data processing |
| ML Orchestration | MLflow | >=3.0 | Experiment tracking + model registry |
| Data Validation | Great Expectations | >=1.3 | Dataset contract enforcement |
| Drift Detection | Evidently AI | >=0.7 | Input-drift monitoring |
| Data Versioning | DVC | >=3.50 | Git-based data pipeline versioning |

### Streaming & Messaging
| Component | Tool | Purpose |
|-----------|------|---------|
| Message Bus | Apache Kafka | One topic, `transactions_raw`, read by two consumer groups |
| Producer | Binance WebSocket API | Real-time trade feed (public, no auth) |
| Serialization | JSON | Schema-mapped messages |

### API & Serving
| Component | Tool | Purpose |
|-----------|------|---------|
| Web Framework | FastAPI | REST API with OpenAPI docs |
| ASGI Server | Uvicorn | High-performance Python server |
| Request Validation | Pydantic | Type-safe schema validation |
| Database ORM | SQLAlchemy 2.0 | PostgreSQL interaction |
| Settings | Pydantic Settings | Environment-based configuration |

### Storage & Data Lake
| Component | Tool | Purpose |
|-----------|------|---------|
| Data Lake | SeaweedFS | S3-compatible object store (Kubernetes-native) |
| RDBMS | PostgreSQL 16 | Prediction logs, feedback, schema versioning |
| Cache | Redis 7 | Online feature store for Feast |
| File Format | Apache Parquet | Columnar at-rest format with Snappy compression |

### Monitoring & Observability
| Component | Tool | Purpose |
|-----------|------|---------|
| Metrics Collection | Prometheus | Time-series metrics scraping |
| Alerting | Alertmanager | Alert routing, deduplication, Slack/webhook |
| Visualization | Grafana | Dashboards for model & infrastructure metrics |
| Logging | stdlib `logging` | Structured per-process logs |

### CI/CD & Orchestration
| Component | Tool | Purpose |
|-----------|------|---------|
| CI/CD | GitHub Actions | Automated testing, training, deployment |
| Containerization | Docker + Docker Compose | Local development & production images |
| Container Orchestration | Kubernetes | Multi-node scaling (drift cronjobs, multiple replicas) |
| Infrastructure as Code | Helm, YAML | SeaweedFS, API, cronjobs declarative configs |
| Public Serving | Render | Zero-cost deployment tier for demos |

### Development Tools
| Component | Tool | Purpose |
|-----------|------|---------|
| Testing | pytest + pytest-asyncio | Unit + integration tests |
| Load Testing | Locust | Distributed API load simulation |
| Linting | ruff | Fast Python linting + formatting |
| Pre-commit Hooks | pre-commit | Enforce code quality on commit |
| Database Migrations | Alembic | Version control for schema |
| Documentation | OpenAPI/Swagger | Auto-generated API docs |

---

## 📋 Prerequisites

Before setting up the project, ensure you have:

### System Requirements
- **OS**: Linux, macOS, or Windows (with WSL2 recommended)
- **Docker**: v24.0+ ([Install Docker](https://docs.docker.com/install/)) with Docker Compose plugin (`docker compose`)
- **Python**: 3.11+ ([Download Python](https://www.python.org/downloads/))
- **Git**: Latest version ([Download Git](https://git-scm.com/))
- **RAM**: Minimum 8GB (recommended 16GB for full stack)
- **Disk**: Minimum 20GB free space

### Required Accounts (for deployment only, not local dev)
- **GitHub**: Account with repository access ([Create GitHub Account](https://github.com/join))
- **Render**: Account for deployment ([Sign up on Render](https://render.com/))
- **HuggingFace**: Account for Space deployment (optional) ([HuggingFace Account](https://huggingface.co/))

### API Access (all free/public)
- **Binance WebSocket**: Public API, no authentication required
  - Docs: [Binance WebSocket API](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)
  - Symbols: BTC/USD, ETH/USD, BNB/USD, SOL/USD, XRP/USD (configurable)

### Optional but Recommended
- **Docker Desktop**: GUI for local container management
- **VS Code**: With Python extension for IDE debugging
- **Postman/Insomnia**: REST client for API testing

---

## ⚙️ Configuration & Setup

### Step 1: Clone the Repository

```bash
git clone https://github.com/YOUR_ORG/real-time-fraud-detector.git
cd real-time-fraud-detector
```

### Step 2: Create `.env` File

Copy the template and customize for your environment:

```bash
cp .env.example .env
```

Edit `.env` with your values.

Important for model serving from MLflow artifacts (SeaweedFS S3):
- Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `.env`.
- These are required by MLflow/boto3 when loading models from `s3://datalake/...`.

### Step 3: Setting Up GitHub Secrets

GitHub Secrets are encrypted environment variables used by GitHub Actions workflows. They're required for deploying containerized models to registries and managing credentials securely.

#### How to Create GitHub Secrets

1. **Navigate to Settings**:
   - Go to your repository: `https://github.com/YOUR_ORG/real-time-fraud-detector`
   - Click **Settings** tab (top right)
   - In the left sidebar, click **Secrets and variables** → **Actions**

2. **Create New Secret**:
   - Click **New repository secret**
   - Enter **Name**: e.g., `DOCKER_REGISTRY_URL`
   - Enter **Value**: e.g., `your-registry.azurecr.io`
   - Click **Add secret**


**Example: Creating a GitHub PAT for container registry access**:

```bash
# 1. Go to: https://github.com/settings/tokens/new
# 2. Check these scopes:
#    ☑ read:packages (pull images)
#    ☑ write:packages (push images)
#    ☑ repo (access private repos)
# 3. Click "Generate token"
# 4. Copy the token and add it as secret GITHUB_TOKEN in your repo
```

## 🚀 Quick Start

Get the entire platform running locally in under 5 minutes:

```bash
# 1. Complete Step 1-3 of "Configuration & Setup" above

# 2. Start all infrastracture services (PostgreSQL, Kafka, Redis, etc.)
make infra-up

# 3. Wait for services to be healthy (~30s)
# Check: docker compose -f docker/docker-compose.yml ps

# 4. Run data ingestion, training, tests, and API
make all

# Note: `make all` now auto-checks whether a Production model exists.
# It runs bootstrap training only when no Production model is registered.
# It also starts Gradio and auto-creates the `mlflow` PostgreSQL database if missing.

# 5. Open your browser:
#    - Fraud API:           http://localhost:8000              (OpenAPI docs)
#    - Gradio Demo:         http://localhost:7860
#    - MLflow Tracking:     http://localhost:5000
#    - Grafana Dashboards:  http://localhost:3000  (admin/admin)
#    - Prometheus:          http://localhost:9090
#    - SeaweedFS Master UI: http://localhost:9333
```

### Verify Everything is Working

```bash
# 1. Check all containers are running
docker compose -f docker/docker-compose.yml ps

# 2. Test the API
curl http://localhost:8000/health

# 3. Check Kafka topics
docker compose -f docker/docker-compose.yml exec kafka \
  kafka-topics --bootstrap-server localhost:9092 --list

# 4. Check data in lake
docker compose -f docker/docker-compose.yml exec seaweedfs-filer \
  ls /datalake/raw/

# 5. View MLflow experiments
open http://localhost:5000
```

---

## 📚 Running the Project

### Running Individual Pipeline Stages

#### **1. Data Ingestion**

```bash
# Start Binance WebSocket → Kafka producer (real-time)
docker compose -f docker/docker-compose.yml up -d binance-producer

# View logs
docker compose -f docker/docker-compose.yml logs -f binance-producer

# Start Kafka → SeaweedFS lake consumer (batched writing)
docker compose -f docker/docker-compose.yml up -d lake-consumer

# View ingestion metrics
curl http://localhost:8000/ingest/status
```

**Configuration**:
- `$BINANCE_SYMBOLS`: Comma-separated trading pairs (default: `btcusdt,ethusdt,bnbusdt,solusdt,xrpusdt`)
- `$LAKE_BATCH_SIZE`: Flush after N messages (default: 1000)
- `$LAKE_BATCH_TIMEOUT_SECS`: OR after N seconds (default: 300)

#### **2. Features, Dataset & Training**

The stages run in this order. Each is a one-shot compose job (`--rm`), so the
host needs no Python environment for them:

```bash
make seed       # one-time: synthetic labelled bootstrap data (empty lake only)
make features   # velocity features → Feast offline store, materialised to Redis
make data       # lake → validated/featured data/dataset.parquet
make train      # Optuna HPO → quality gate → MLflow registration
```

`make bootstrap-model` chains all four and then promotes the run. Training
never promotes by itself:

```bash
RUN_ID=$(python -c "import json;print(json.load(open('metrics.json'))['run_id'])")
docker compose -f docker/docker-compose.yml run --rm trainer \
  python scripts/model_ctl.py promote --run-id "$RUN_ID" --stage Production
```

**Training Configuration** (edit `params.yaml`):
```yaml
model:
  n_trials: 24            # Optuna trials
  n_estimators_min: 150
  n_estimators_max: 400
  max_depth_max: 8
  learning_rate_min: 0.005
  learning_rate_max: 0.2
  early_stopping_rounds: 30
  min_auc: 0.75           # quality gate
  min_avg_prec: 0.30      # quality gate (PR-AUC)

dataset:
  max_files: 20           # newest raw parquet files to read
  max_rows: 250000
  min_labeled_rows: 500   # refuse to train below this
```

**Output**:
- `metrics.json` — test-split metrics, the chosen threshold, run id, model version
- Model artifact under `s3://datalake/models/`, registered as `fraud_detector`
- The registered version carries a `decision_threshold` tag used by serving

#### **3. Real-Time API & Serving**

```bash
# Start FastAPI server
make serve

# Or with custom host/port:
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

# View OpenAPI docs
open http://localhost:8000/docs

# Make a prediction
curl -X POST http://localhost:8000/predict/ \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "txn_123",
    "timestamp": "2024-06-15T14:32:07Z",
    "user_id": "usr_001",
    "merchant_id": "mrch_001",
    "merchant_cat": "electronics",
    "amount_usd": 49.99,
    "currency": "USD",
    "country": "US",
    "device_type": "mobile",
    "ip_hash": "aabb1122",
    "card_last4": "1234",
    "is_international": false,
    "hour_of_day": 14,
    "day_of_week": 2
  }'

# Response example:
# {
#   "transaction_id": "txn_123",
#   "fraud_probability": 0.12,
#   "decision": "APPROVE",
#   "model_version": "2",
#   "shadow_probability": 0.15,
#   "latency_ms": 45.3
# }

# Check ingestion status
curl http://localhost:8000/ingest/status | jq .

# Get Prometheus metrics
curl http://localhost:8000/metrics | grep fraud_
```

**Key Features**:
- **Cached model, vectorized inference**: the model is loaded once and refreshed on a TTL
- **Shadow routing**: the challenger stage is sampled on a configurable fraction of traffic
- **Prediction logging**: every prediction is stored in PostgreSQL, keyed by `transaction_id`
- **Prometheus metrics**: `predictions_total`, `prediction_latency_seconds`, `fraud_probability_last`, `feature_store_errors_total`

#### **4. Kafka Consumer for Async Scoring**

```bash
# Start prediction consumer (reads transactions_raw, scores, logs to SQL)
docker compose -f docker/docker-compose.yml up -d kafka-consumer

# View worker logs
docker compose -f docker/docker-compose.yml logs -f kafka-consumer

# Check consumer lag
docker compose -f docker/docker-compose.yml exec kafka \
  kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group prediction-workers --describe
```

#### **5. Data Validation with Great Expectations**

```bash
# Lake readiness + validation of the newest raw batch (what CI runs)
bash scripts/pipeline_gate.sh

# Validate one parquet file — local path or s3:// URI
python gx/validate.py s3://datalake/raw/realtime_txn_<timestamp>.parquet
```

The suite in `gx/expectations/` is the *training dataset contract*; it also runs
inside `make data`, where a failure blocks training instead of only warning.

#### **6. Drift Detection & Alerting**

```bash
# Compare the last day of predictions against the previous 13 days
make drift

# Or manually:
python monitoring/run_drift.py

# Reports are written to /tmp/drift_reports/<YYYY-MM-DD>/ and uploaded to
# s3://datalake/drift_reports/<YYYY-MM-DD>/
ls /tmp/drift_reports/

# Check Prometheus drift alert rules
curl http://localhost:9090/api/v1/rules | jq '.data.groups[] | select(.name=="drift_alerts")'
```

#### **7. Interactive Demo (Gradio)**

```bash
# Start Gradio web interface
docker compose -f docker/docker-compose.yml up -d gradio-demo

# Open http://localhost:7860 in browser
# Input transaction details → Get real-time prediction
```

---

## 📁 Project Structure

```
real-time-fraud-detector/
├── api/                          # FastAPI application
│   ├── main.py                   # Entry point, lifespan warm-up, routers
│   ├── model_loader.py           # Registry-backed model cache (TTL refresh)
│   ├── inference.py              # Shared scoring path (API + consumer)
│   ├── metrics.py                # Prometheus metrics definitions
│   ├── shadow.py                 # Champion/challenger routing
│   └── routers/
│       ├── predict.py            # POST /predict endpoint
│       └── ingest_status.py      # GET /ingest/status lake counts
│
├── training/                     # ML training pipeline
│   ├── build_dataset.py          # lake → validated + featured dataset
│   ├── train.py                  # Optuna HPO + XGBoost + quality gate
│   └── feature_engineering.py    # Deterministic features + FEATURE_COLS
│
├── ingestion/                    # Event streaming
│   ├── producer.py               # Binance WebSocket → Kafka (transactions_raw)
│   ├── lake_consumer.py          # Kafka → SeaweedFS batch writer
│   └── predict_consumer.py       # Prediction consumer (async scoring)
│
├── datalake/                     # S3 data-lake access layer
│   └── storage.py                # client, parquet IO, lake_status()
│
├── features/                     # Feature layer
│   ├── compute.py                # Hourly velocity aggregates → Feast sources
│   ├── schema.py                 # Velocity feature names (no Feast import)
│   └── store.py                  # Feast offline/online access
│
├── db/                           # Database layer
│   ├── models.py                 # SQLAlchemy ORM (predictions table)
│   └── writer.py                 # Idempotent prediction inserts
│
├── alembic/                      # Database version control
│   ├── env.py                    # Alembic configuration
│   ├── script.py.mako            # Migration template
│   └── versions/
│       ├── 20260404_0001_init_schema.py
│       └── 20260407_0002_predictions_unique_txn.py
│
├── feature_repo/                 # Feast repository
│   ├── feature_store.yaml        # Registry, Redis online store, file offline store
│   └── features.py               # user_stats / merchant_stats views
│
├── gx/                           # Data validation (Great Expectations)
│   ├── validate.py               # Suite runner (ephemeral context)
│   └── expectations/
│       └── fraud_transactions_suite.json  # Dataset contract
│
├── config/                       # Configuration management
│   ├── settings.py               # Pydantic BaseSettings (env vars)
│   └── params.py                 # Typed params.yaml loader
│
├── monitoring/                   # Observability stack
│   ├── run_drift.py              # Evidently drift job → Pushgateway
│   ├── alert_webhook.py          # Authenticated Alertmanager → GitHub dispatch
│   ├── prometheus/
│   │   ├── prometheus.yml        # Prometheus scrape config
│   │   └── rules/
│   │       └── drift.yml         # Alert rules
│   ├── grafana/
│   │   └── dashboards/
│   │       └── overview.json
│   └── alertmanager/
│       └── alertmanager.yml      # Alert routing config
│
├── docker/                       # Containerization
│   ├── docker-compose.yml        # Full local stack (22 services, 3 job profiles)
│   ├── Dockerfile.api            # FastAPI + consumers container
│   ├── Dockerfile.gradio         # Gradio demo container
│   ├── Dockerfile.training       # Training + MLflow container
│   └── Dockerfile.monitoring     # Evidently + webhook container
│
├── k8s/                          # Kubernetes manifests
│   ├── mlops/
│   │   ├── fraud-api-deployment.yaml
│   │   └── drift-cronjob.yaml
│   └── seaweedfs/
│       ├── values.yaml           # Helm values
│       └── secret.yaml           # RBAC credentials
│
├── tests/                        # Test suite
│   ├── unit/
│   │   ├── test_api.py           # Endpoint tests
│   │   ├── test_build_dataset.py # Dataset stage contract
│   │   ├── test_feature_engineering.py
│   │   ├── test_gx_suite.py      # Great Expectations suite
│   │   ├── test_scorer.py        # Shared scoring path
│   │   └── test_storage_status.py# Lake status counts
│   ├── integration/
│   │   └── test_ingestion_e2e.py # Offline producer → model-input round trip
│   └── load/
│       └── locustfile.py         # Distributed load testing
│
├── scripts/                      # Utility scripts
│   ├── ingestion_ctl.py          # Lake status / newest batch / seed
│   ├── model_ctl.py              # Model registry exists + promote
│   ├── pipeline_gate.sh          # Pre-training lake readiness gate
│   ├── wait_for_postgres.py      # Healthcheck for DB startup
│   └── wait_for_s3.py            # S3 endpoint readiness + bucket bootstrap
│
├── .github/
│   └── workflows/                # GitHub Actions CI/CD
│       ├── ci.yml                # pre-commit + unit tests + image check
│       ├── _train.yml            # Reusable training pipeline
│       ├── train.yml             # Push-triggered training → Staging
│       ├── retrain.yml           # Weekly/manual retraining → Production
│       └── deploy.yml            # Render deploy (RENDER_DEPLOY gated)
│
├── .dvc/                         # Data Version Control
│   └── config                    # S3 backend configuration
│
├── alembic.ini                   # Alembic root config
├── dvc.yaml                      # DVC pipeline stages
├── params.yaml                   # Trial search space + quality gates
├── requirements.txt              # Production dependencies
├── requirements/dev.txt          # Development + testing deps
├── pyproject.toml                # Project metadata, ruff + coverage config
├── Makefile                      # Automation commands
├── .env.example                  # Environment template
├── .gitignore                    # Git exclusions
├── render.yaml                   # Render deployment config
└── app.py                        # Gradio demo UI
```

---

## 🧪 Testing

### Running Tests

```bash
# Unit + integration tests (no services required)
make test

# Outside an activated virtualenv, point make at the project interpreter:
make test PYTHON=.venv/Scripts/python.exe    # Windows
make test PYTHON=.venv/bin/python            # macOS / Linux

# Individual suites
pytest tests/unit -v
pytest tests/integration -v

# With coverage (source list lives in pyproject.toml)
pytest tests/unit --cov

# Load tests (requires the API to be running)
locust -f tests/load/locustfile.py --host http://localhost:8000
```

### Test Categories

| Level | Framework | Location | What | When | Command |
|-------|-----------|----------|------|------|---------|
| **Unit** | pytest | `tests/unit/` | API, feature contract, GX suite, dataset build, scorer, lake status | Every push | `pytest tests/unit` |
| **Integration** | pytest | `tests/integration/` | Producer → lake pre-checks → model input, offline | On merge | `pytest tests/integration` |
| **Load** | Locust | `tests/load/` | API throughput and p99 latency | Before deploy | `locust ...` |

There is no separate "model quality" pytest suite: the model quality gate lives
in `training/train.py`, where it actually blocks registration.

Model quality gate (in `train.py`):
- `auc_roc >= model.min_auc` and `avg_prec >= model.min_avg_prec` on the held-out test split
- a failing model is never registered, so it can never be promoted
- the F1-maximising threshold from the validation split is stored on the model version

---

## 📊 Monitoring & Observability

### Dashboards & Tools Access

| Service | URL | Purpose | Credentials |
|---------|-----|---------|-------------|
| **Fraud API Docs** | http://localhost:8000/docs | OpenAPI interactive documentation | None |
| **Gradio Demo** | http://localhost:7860 | Web UI for predictions | None |
| **MLflow UI** | http://localhost:5000 | Experiment tracking & model registry | None |
| **SeaweedFS master** | http://localhost:9333 | Data lake management | None |
| **SeaweedFS Filer** | http://localhost:8888 | Browse data lake contents | None |
| **Grafana** | http://localhost:3000 | Metric dashboards | `admin` / `admin` |
| **Prometheus** | http://localhost:9090 | Metrics & alert rules | None |
| **Alertmanager** | http://localhost:9093 | Alert routing & history | None |

### Key Metrics Being Tracked

Only metrics an actual process sets are listed here; anything else would be a
dashboard that never moves.

```
# API (api/metrics.py, scraped from fraud-api:8000)
- predictions_total                       # Counter: by decision, model_version
- prediction_latency_seconds              # Histogram: request latency
- fraud_probability_last                  # Gauge: last served probability
- feature_store_errors_total              # Counter: online lookups that fell back to defaults

# Consumers (each process exports its own registry)
- lake_rows_written_total                 # Counter: rows landed in raw/ (:8002)
- lake_batches_written_total              # Counter: batches landed in raw/ (:8002)
- consumer_messages_processed_total       # Counter: messages scored (:9101)
- consumer_duplicates_total               # Counter: replays skipped by the unique index
- consumer_errors_total                   # Counter: scoring/persistence failures
- kafka_consumer_lag                      # Gauge: messages behind the topic head (per consumer)

# Drift (pushed by monitoring/run_drift.py via Pushgateway)
- evidently_drift_share                   # Gauge: share of input columns drifted
```

---

## 🏷️ Dataset & labels

The Binance feed is a public trade stream: it carries **no fraud labels**. Every
record the producer publishes is unlabelled, and the lake consumer never invents
one.

Labels enter the pipeline through exactly one path — the synthetic seed
(`make seed` → `scripts/ingestion_ctl.py seed`), which writes labelled rows to
`s3://datalake/raw/seed_*.parquet`. Training reads those labelled batches
explicitly (`dataset.max_files` newest of them), so the unlabelled live batches
the producer keeps appending never push the labelled data out of the window.
Live traffic is used for features and drift detection, not for fitting.

Consequently the classifier is a **demonstration trained on synthetic labels**,
not a production fraud model. `training/build_dataset.py` refuses to train when
there are fewer than `dataset.min_labeled_rows` labelled rows or only one class,
and names `make seed` in the error.

## 🔁 Retraining switch

Scheduled retraining is controlled by a repository variable, not by editing the
workflow:

| Setting | Weekly scheduled run | Manual dispatch | Drift-webhook dispatch |
|---------|----------------------|-----------------|------------------------|
| `AUTO_RETRAIN_ENABLED` unset (default) | skipped, sends no email | runs | runs |
| `AUTO_RETRAIN_ENABLED=true` | runs, promotes to `Production` | runs | runs |

Set it under **Settings → Secrets and variables → Actions → Variables**. A job
skipped by its `if:` condition still appears as a run, but GitHub only emails for
*failed* runs, so a muted schedule stays silent.

Retraining is event-driven first: drift alerts dispatch the workflow, and the
weekly cron is only the fallback you can mute without touching code.
