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

---

## 🎯 Project Overview

This MLOps platform demonstrates a **professional-grade production pipeline** for fraud detection featuring:

### Core Capabilities

- **Real-Time Data Ingestion**: Live Binance WebSocket trade feed (public API, no authentication required) streamed via Apache Kafka
- **Data Lake**: S3-compatible SeaweedFS for unified data storage across all pipeline stages
- **Feature Engineering**: Automated feature computation with Feast (offline + online store backed by Redis)
- **ML Training**: XGBoost with Optuna hyperparameter optimization, MLflow experiment tracking and model registry
- **Model Validation**: Great Expectations schema & statistical validation, deepchecks behavioral testing
- **Real-Time Serving**: FastAPI REST API with sub-100ms latency and shadow/canary deployment patterns
- **Prediction Logging**: PostgreSQL-backed audit trail for compliance and feedback loops
- **Data Drift Detection**: Evidently AI statistical drift reports with automated retraining triggers
- **Monitoring & Alerting**: Prometheus + Grafana dashboards with Alertmanager webhook integration
- **CI/CD Pipeline**: GitHub Actions for automated testing, training, and deployment on push/schedule
- **Data Versioning**: DVC integration for reproducible ML workflows

### Sample Domain

Fraud detection on crypto exchange trades (synthesized from Binance public WebSocket feed) with features derived in real-time:
- Transaction metadata: amount, currency pair, device type, IP geography
- Time-based aggregation: hourly fraud rates, velocity checks
- User-level patterns: historical fraud ratio, transaction frequency

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
- **Binance WebSocket Producer** (`kafka/producer.py`): Real-time trade feed to Kafka with schema mapping and deterministic field derivation
- **Lake Consumer** (`kafka/lake_consumer.py`): Kafka → SeaweedFS batch writer with configurable flush windows
- **Great Expectations** (`gx/validate.py`): Statistical & schema validation before lake persistence
- **Feast Feature Store** (`feast/features.py`, `feature_store.yaml`): Offline & online feature computation

### Model Training Pipeline
- **Feature Engineering** (`training/feature_engineering.py`): Deterministic feature construction, scaling, encoding
- **Training Script** (`training/train.py`): XGBoost + Optuna with:
  - Bayesian hyperparameter optimization (configurable, default 24 trials)
  - Imbalanced fraud detection handling (scale_pos_weight)
  - Holdout validation and model promotion to `Production`
  - MLflow automatic logging
- **Deepchecks Integration**: Behavioral model checks (drift, performance)
- **Model Registry**: MLflow promotion workflow (staging → production)

### Real-Time Serving
- **FastAPI API** (`api/main.py`): Production-grade REST endpoints
  - `POST /predict/` — Single transaction scoring with sub-100ms latency
  - `GET /ingest/status` — Kafka consumer lag & lake ingestion metrics
  - `GET /health` — Liveness probe for Kubernetes
  - `GET /metrics` — Prometheus metrics endpoint
- **Prediction Consumer** (`kafka/consumer.py`): Kafka worker group for asynchronous scoring
- **Model Loader** (`api/model_loader.py`): Automatic model retrieval from MLflow with caching
- **Shadow Routing** (`api/shadow.py`): Canary/champion deployment for safe model transitions
- **Prediction Logging** (`db/models.py`): PostgreSQL audit trail for compliance

### Monitoring & Observability
- **Prometheus** (`monitoring/prometheus/prometheus.yml`): Metrics scraping from API
- **Grafana** (`monitoring/grafana/dashboards/overview.json`): Dashboards for:
  - Prediction latency & throughput
  - Fraud rate trends
  - Model version tracking
  - Kafka consumer lag
- **Evidently AI** (`monitoring/run_drift.py`): Data & model drift detection with alerts
- **Alertmanager** (`monitoring/alertmanager/alertmanager.yml`): Alert routing & aggregation
- **Alert Webhook** (`monitoring/alert_webhook.py`): Custom actions on drift detection (retrain triggers)

### Testing & Quality Assurance
- **Unit Tests** (`tests/unit/`): API endpoints, feature engineering, model loading
- **Integration Tests** (`tests/integration/`): End-to-end ingestion pipeline
- **Model Quality Tests** (`tests/model/`): Behavioral checks on trained models
- **Load Testing** (`tests/load/locustfile.py`): Distributed load simulation with Locust

### CI/CD & Orchestration
- **GitHub Actions** (`.github/workflows/`):
  - `ci.yml` — Unit & integration tests on every push
  - `train.yml` — Train & quality gates on training/data changes or manual trigger
  - `deploy.yml` — Docker build & push to registry
  - `retrain.yml` — Scheduled (every 6h) + manual retraining, gated by minimum lake data
- **DVC Pipeline** (`dvc.yaml`): Data versioning + reproducible training
- **Makefile**: One-command setup & orchestration
- **Docker Compose**: Full local stack (17 services, including monitoring and Gradio)

### Database Migrations
- **Alembic** (`alembic/`): Schema versioning with auto-generated migrations
- **SQL Migrations** (`db/migrations/001_init.sql`): Fallback manual schema

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
| Hyperparameter Optimization | Optuna | >=3.6 | Bayesian search with pruning |
| Feature Store | Feast | >=0.38 | Offline + online feature registry |
| Feature Computation | pandas + PyArrow | 2.2+ / 15.0+ | Columnar data processing |
| ML Orchestration | MLflow | >=2.13 | Experiment tracking + model registry |
| Data Validation | Great Expectations | >=0.18 | Schema + statistical validation |
| Model Testing | deepchecks | >=0.18 | Behavioral model quality gates |
| Drift Detection | Evidently AI | >=0.4 | Data & model drift monitoring |
| Data Versioning | DVC | >=3.50 | Git-based data pipeline versioning |

### Streaming & Messaging
| Component | Tool | Purpose |
|-----------|------|---------|
| Message Bus | Apache Kafka | 3 topics for transactions_raw, transactions, predictions |
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
| Tracing | structlog | Structured JSON logging |

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
| Testing | pytest + pytest-asyncio | Unit, integration, model quality tests |
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

#### **2. Training & Hyperparameter Optimization**

```bash
# Run full training pipeline with Optuna HPO
make train

# Or manually:
python training/train.py

# Track progress in MLflow UI
open http://localhost:5000

# View training metrics
cat metrics.json | jq .
```

**Training Configuration** (edit `params.yaml`):
```yaml
seed: 42
n_rows: 100000          # Rows to sample from lake
fraud_rate: 0.02        # Expected fraud rate (for sampling)

model:
  n_trials: 24          # Optuna trials
  n_estimators_min: 150
  n_estimators_max: 400
  max_depth_max: 8
  learning_rate_min: 0.005
  learning_rate_max: 0.2
```

**Output**:
- Model artifact: `s3://datalake/models/<run_id>/model.pkl`
- Registered in MLflow: `fraud_detector` (model registry)
- Promoted to `Production` stage if metrics pass gates

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
#   "shadow_prob": 0.15,
#   "latency_ms": 45.3
# }

# Check ingestion status
curl http://localhost:8000/ingest/status | jq .

# Get Prometheus metrics
curl http://localhost:8000/metrics | grep fraud_
```

**Key Features**:
- **Sub-100ms latency**: Cached model, vectorized inference
- **Shadow routing**: Compare current + challenger models
- **Prediction logging**: All predictions stored in PostgreSQL
- **Prometheus metrics**: `predictions_total`, `prediction_latency`, `fraud_prob_gauge`

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
# Run validation on lake data
bash scripts/pipeline_gate.sh

# Or manually trigger validation
python gx/validate.py

# View validation report
open gx/validation_report.html
```

#### **6. Drift Detection & Alerting**

```bash
# Run Evidently drift detection
make drift

# Or manually:
python monitoring/run_drift.py

# View drift report
open monitoring/drift_report.html

# Check Prometheus drift alert rules
curl http://localhost:9090/api/v1/rules | jq '.data.groups[] | select(.name=="drift")'
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
│   ├── main.py                   # Entry point, middleware, routers
│   ├── model_loader.py           # MLflow model loading + caching
│   ├── metrics.py                # Prometheus metrics definitions
│   ├── shadow.py                 # Shadow routing for canary deployments
│   └── routers/
│       ├── predict.py            # POST /predict endpoint
│       └── ingest_status.py      # GET /ingest/status metrics
│
├── training/                     # ML training pipeline
│   ├── train.py                  # Optuna HPO + XGBoost training
│   └── feature_engineering.py    # Deterministic feature computation
│
├── kafka/                        # Event streaming
│   ├── producer.py               # Binance WebSocket → Kafka (transactions_raw)
│   ├── lake_consumer.py          # Kafka → SeaweedFS batch writer
│   └── consumer.py               # Prediction consumer (async scoring)
│
├── db/                           # Database layer
│   └── models.py                 # SQLAlchemy ORM (predictions table)

│
├── alembic/                      # Database version control
│   ├── env.py                    # Alembic configuration
│   ├── script.py.mako            # Migration template
│   └── versions/
│       └── 20260404_0001_init_schema.py
│
├── feast/                        # Feature store
│   ├── feature_store.yaml        # Feast registry (offline + online)
│   └── features.py               # Feature definitions
│
├── gx/                           # Data validation (Great Expectations)
│   ├── validate.py               # GX Checkpoint runner
│   ├── expectations/
│   │   └── fraud_transactions_suite.json  # Validation rules
│   └── checkpoints/
│
├── config/                       # Configuration management
│   └── settings.py               # Pydantic BaseSettings (env vars)
│
├── monitoring/                   # Observability stack
│   ├── run_drift.py              # Evidently drift detection job
│   ├── alert_webhook.py          # Alertmanager → custom actions
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
│   ├── docker-compose.yml        # Full local stack (17 services)
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
│   │   └── test_feature_engineering.py
│   ├── integration/
│   │   └── test_ingestion_e2e.py # End-to-end pipeline
│   ├── model/
│   │   └── test_model_quality.py # deepchecks behavioral tests
│   ├── load/
│   │   └── locustfile.py         # Distributed load testing
│   └── contract/                 # API contract tests
│
├── scripts/                      # Utility scripts
│   ├── ingestion_ctl.py          # Ingestion control & retry
│   ├── model_ctl.py              # Model registry operations
│   ├── pipeline_gate.sh          # Pre-training validation gates
│   ├── wait_for_postgres.py      # Healthcheck for DB startup
│   └── wait_for_s3.py            # S3 endpoint readiness + bucket bootstrap
│
├── .github/
│   └── workflows/                # GitHub Actions CI/CD
│       ├── ci.yml                # Unit + integration tests
│       ├── train.yml             # Scheduled training job
│       ├── deploy.yml            # Docker image build & push
│       └── retrain.yml           # Drift-triggered retraining
│
├── .dvc/                         # Data Version Control
│   └── config                    # S3 backend configuration
│
├── alembic.ini                   # Alembic root config
├── dvc.yaml                      # DVC pipeline stages
├── params.yaml                   # Hyperparameter config
├── requirements.txt              # Production dependencies
├── requirements-dev.txt          # Development + testing deps
├── pyproject.toml                # Python project metadata
├── Makefile                      # Automation commands
├── .env.example                  # Environment template
├── .gitignore                    # Git exclusions
├── render.yaml                   # Render deployment config
└── app.py                        # Gradio demo UI
```

---

## 🧪 Testing

The project includes comprehensive testing at multiple levels:

### Running Tests

```bash
# Run all tests
make test

# Run specific test suite
pytest tests/unit -v
pytest tests/integration -v
pytest tests/model -v

# Run with coverage
pytest --cov=api --cov=training --cov=kafka tests/

# Run load tests (simulates 100 concurrent users)
locust -f tests/load/locustfile.py --host http://localhost:8000
```

### Test Categories

| Level | Framework | Location | What | When | Command |
|-------|-----------|----------|------|------|---------|
| **Unit** | pytest | `tests/unit/` | API endpoints, feature eng, model loader | Every push | `pytest tests/unit` |
| **Integration** | pytest | `tests/integration/` | Ingestion → lake → training pipeline | On merge | `pytest tests/integration` |
| **Model Quality** | deepchecks | `tests/model/` | Drift detection, label leakage, performance degradation | Post-training | `pytest tests/model` |
| **Load** | Locust | `tests/load/` | API throughput, p99 latency under 100 concurrent users | Before deploy | `locust ...` |

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

```
# API Performance
- predictions_total                    # Counter: total predictions by model_version, decision
- prediction_latency                   # Histogram: inference time in ms
- fraud_prob_gauge                     # Gauge: latest fraud probability

# Kafka
- kafka_consumer_lag                   # Gauge: lag per consumer group
- messages_processed_total             # Counter: messages by topic

# Data Quality
- fraud_rate_hourly                    # Gauge: hourly fraud rate %
- data_drift_detected                  # Alert: statistical drift > threshold

# Model
- model_version_active                 # Gauge: current production model ID
- prediction_decision_distribution     # Counter: APPROVE vs REJECT
```
