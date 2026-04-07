# Testing

```bash
make test                                   # unit + integration, no services required

make test PYTHON=.venv/Scripts/python.exe   # Windows, venv not activated
make test PYTHON=.venv/bin/python           # macOS / Linux

pytest tests/unit -v
pytest tests/integration -v
pytest tests/unit --cov                     # source list lives in pyproject.toml

locust -f tests/load/locustfile.py --host http://localhost:8000
```

| Level | Location | Covers |
|---|---|---|
| Unit | `tests/unit/` | API endpoints, model-loader cache, feature contract, GX suite, dataset build, scorer, lake status, consumer lag, CLI selection |
| Integration | `tests/integration/` | Producer mapping → lake pre-checks → model input, fully offline |
| Load | `tests/load/` | `/predict/` throughput and p99 under Locust |

The suites need no containers: external systems are stubbed at the module
boundary (`record_prediction`, `shadow_router.predict`, the MLflow handle), so a
failure points at the code rather than at a missing service.

## What is deliberately not a test

There is no "model quality" pytest suite. The gate lives in `training/train.py`,
where it decides whether a version is registered:

- `auc_roc >= model.min_auc` and `avg_prec >= model.min_avg_prec` on the held-out
  test split;
- a failing model is never registered, so it can never be promoted;
- the F1-maximising threshold from validation is stored on the version.

Testing that gate by asserting a number in a unit test would prove nothing about
the pipeline; running the pipeline proves it.

## CI

| Workflow | Trigger | Runs |
|---|---|---|
| `ci.yml` | push / PR | `pre-commit run --all-files` (ruff lint + format + hygiene), unit tests with coverage, `import api.main` |
| `_train.yml` | called by the two below | pipeline gate → `features/compute.py` → `dvc repro` → gate → register |
| `train.yml` | push to `main`, manual | calls `_train.yml`, stage `Staging` |
| `retrain.yml` | weekly cron, manual, drift webhook | calls `_train.yml`, stage `Production`; the scheduled run is gated by `AUTO_RETRAIN_ENABLED` |
| `deploy.yml` | after a successful `Train Pipeline` | Render deploy, gated by `RENDER_DEPLOY` |

```bash
pre-commit run --all-files
.venv/bin/python -m ruff check . && .venv/bin/python -m ruff format --check .
```

`scripts/pipeline_gate.sh` is the CI entry point for lake readiness: it reports
the lake state, seeds a synthetic batch only when no labelled batch exists, and
validates the newest labelled batch against the Great Expectations suite.
