# Training

```
lake (labelled batches) ──► build_dataset.py ──► data/dataset.parquet
                                   │                        │
                              GX contract               train.py
                                                            │
                                        Optuna + XGBoost ───┤
                                        quality gate ───────┤ (fails ⇒ nothing registered)
                                        mlflow.xgboost ─────┴──► registry: fraud_detector
```

Two DVC stages (`dvc.yaml`), each a compose job:

```bash
make data    # dvc repro build_dataset
make train   # dvc repro train
```

`dvc repro` skips a stage whose inputs are unchanged, so repeated runs are cheap
and reproducible.

## Dataset build

`training/build_dataset.py`:

1. reads the newest `dataset.max_files` **labelled** batches (`raw/seed_*.parquet`),
2. samples to `dataset.max_rows` (`random_state=42`),
3. drops unlabelled rows and refuses to continue below `dataset.min_labeled_rows`
   or with a single class,
4. runs the Great Expectations suite — a failure exits non-zero,
5. joins Feast point-in-time velocity features, fills cold-entity defaults,
6. writes `data/dataset.parquet`.

Unlabelled live batches are never training data, so they cannot push the
labelled window out as the producer keeps writing.

## Training

`training/train.py`:

- stratified 60/20/20 split;
- Optuna TPE search (`model.n_trials`) over depth, learning rate, subsampling…;
- XGBoost with `aucpr` as the objective and early stopping on the validation
  split (`model.early_stopping_rounds`);
- an F1-maximising decision threshold scanned on validation and stored as the
  `decision_threshold` tag on the model version;
- a **quality gate** on the held-out test split, evaluated before logging.

Metrics land in `metrics.json`:

```json
{
  "auc_roc": 0.9983, "avg_prec": 0.8183, "f1_at_threshold": 0.9231,
  "precision": 0.8571, "recall": 1.0, "threshold": 0.23,
  "run_id": "…", "model_version": "1",
  "n_train": 1800, "n_valid": 600, "n_test": 600,
  "label_rate": 0.02, "n_features": 17
}
```

The model is logged with the **xgboost flavor** (`model.ubj`), which keeps the
artifact pickle-free and avoids skops' untrusted-type refusal for XGBClassifier.

## Quality gates

`params.yaml`:

```yaml
model:
  n_trials: 24
  n_estimators_min: 150
  n_estimators_max: 400
  max_depth_max: 8
  learning_rate_min: 0.005
  learning_rate_max: 0.2
  early_stopping_rounds: 30
  min_auc: 0.75           # gate
  min_avg_prec: 0.30      # gate (PR-AUC, the right metric at a ~2% base rate)

dataset:
  max_files: 20           # newest labelled batches
  max_rows: 250000
  min_labeled_rows: 500   # refuse to train below this
```

A model that misses either gate prints `QUALITY GATE FAILED …` and exits before
`log_model`, so no version is created.

## Promotion

Training never promotes. Only the operator CLI moves a version:

```bash
RUN_ID=$(python -c "import json;print(json.load(open('metrics.json'))['run_id'])")
docker compose -f docker/docker-compose.yml run --rm trainer \
  python scripts/model_ctl.py promote --run-id "$RUN_ID" --stage Production

# is anything in Production?
docker compose -f docker/docker-compose.yml run --rm trainer \
  python scripts/model_ctl.py exists --stage Production
```

`promote` archives the previous Production version, so exactly one version
serves at a time. `make bootstrap-model` chains seed → features → data → train →
promote; `make all` calls it only when no Production model exists.

## Dataset & labels

The Binance feed is a public trade stream: it carries **no fraud labels**. Every
record the producer publishes is unlabelled, and the lake consumer never invents
one.

Labels enter the pipeline through exactly one path — the synthetic seed
(`make seed` → `scripts/ingestion_ctl.py seed`), which writes labelled rows to
`s3://datalake/raw/seed_*.parquet`. Training reads those labelled batches
explicitly (`dataset.max_files` newest of them), so the unlabelled live batches
the producer keeps appending never push the labelled data out of the window.
Live traffic is used for features and drift detection, not for fitting.

The classifier is therefore a **demonstration trained on synthetic labels**, not
a production fraud model. `training/build_dataset.py` refuses to train when
there are fewer than `dataset.min_labeled_rows` labelled rows or only one class,
and names `make seed` in the error.

## Retraining switch

Scheduled retraining is controlled by a repository variable, not by editing the
workflow:

| Setting | Weekly scheduled run | Manual dispatch | Drift-webhook dispatch |
|---|---|---|---|
| `AUTO_RETRAIN_ENABLED` unset | skipped, sends no email | runs | runs |
| `AUTO_RETRAIN_ENABLED=true` | runs, promotes to `Production` | runs | runs |

Set it under **Settings → Secrets and variables → Actions → Variables**. A job
skipped by its `if:` still appears as a run, but GitHub only emails for failed
runs, so a muted schedule stays silent.

Retraining is event-driven first: drift alerts dispatch the workflow, and the
weekly cron is only the fallback you can mute without touching code.
