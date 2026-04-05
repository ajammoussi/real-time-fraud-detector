"""Main training entrypoint with MLflow + Optuna."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import mlflow
import numpy as np
import optuna
import pandas as pd
import s3fs
from mlflow.tracking import MlflowClient
from sklearn.metrics import average_precision_score, f1_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

from config.settings import get_settings
from training.feature_engineering import engineer_features, get_model_input_frame

cfg = get_settings()
optuna.logging.set_verbosity(optuna.logging.WARNING)

_DEFAULT_TRAINING_PARAMS = {
    "n_trials": 24,
    "n_estimators_min": 150,
    "n_estimators_max": 400,
    "max_depth_max": 8,
    "learning_rate_min": 5e-3,
    "learning_rate_max": 0.2,
    "early_stopping_rounds": 30,
    "max_files": 20,
    "max_rows": 250_000,
}


def _load_training_params(params_path: str = "params.yaml") -> dict:
    """Load training search-space settings from params.yaml with safe defaults."""
    params = _DEFAULT_TRAINING_PARAMS.copy()
    p = Path(params_path)
    if not p.exists():
        return params

    try:
        import yaml
    except Exception:
        return params

    raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    model_cfg = raw.get("model", {}) if isinstance(raw, dict) else {}

    params["n_trials"] = int(model_cfg.get("n_trials", params["n_trials"]))
    params["n_estimators_min"] = int(
        model_cfg.get("n_estimators_min", params["n_estimators_min"])
    )
    params["n_estimators_max"] = int(
        model_cfg.get("n_estimators_max", params["n_estimators_max"])
    )
    params["max_depth_max"] = int(
        model_cfg.get("max_depth_max", params["max_depth_max"])
    )
    params["learning_rate_min"] = float(
        model_cfg.get("learning_rate_min", params["learning_rate_min"])
    )
    params["learning_rate_max"] = float(
        model_cfg.get("learning_rate_max", params["learning_rate_max"])
    )
    params["early_stopping_rounds"] = int(
        model_cfg.get("early_stopping_rounds", params["early_stopping_rounds"])
    )
    params["max_files"] = int(model_cfg.get("max_files", params["max_files"]))
    params["max_rows"] = int(model_cfg.get("max_rows", params["max_rows"]))
    return params


TRAINING_PARAMS = _load_training_params()


def load_lake_training_data() -> pd.DataFrame:
    fs = s3fs.S3FileSystem(
        key=cfg.seaweed_access_key,
        secret=cfg.seaweed_secret_key,
        endpoint_url=cfg.seaweed_endpoint,
    )
    files = sorted(fs.glob(f"{cfg.datalake_bucket}/raw/*.parquet"))
    if not files:
        raise FileNotFoundError("No raw parquet files found in the data lake")

    # Keep training latency bounded as lake grows while still learning from recent data.
    max_files = TRAINING_PARAMS["max_files"]
    if max_files > 0 and len(files) > max_files:
        files = files[-max_files:]

    dfs = [pd.read_parquet(f"s3://{f}", filesystem=fs) for f in files]
    df = pd.concat(dfs, ignore_index=True)

    max_rows = TRAINING_PARAMS["max_rows"]
    if max_rows > 0 and len(df) > max_rows:
        df = df.sample(n=max_rows, random_state=42).reset_index(drop=True)

    return df


def objective(trial: optuna.Trial, X_tr, y_tr, X_va, y_va) -> float:
    positive_count = int((y_tr == 1).sum())
    negative_count = int((y_tr == 0).sum())
    scale_pos_weight = (negative_count / positive_count) if positive_count else 1.0

    params = {
        "n_estimators": trial.suggest_int(
            "n_estimators",
            TRAINING_PARAMS["n_estimators_min"],
            TRAINING_PARAMS["n_estimators_max"],
            step=50,
        ),
        "max_depth": trial.suggest_int(
            "max_depth", 3, TRAINING_PARAMS["max_depth_max"]
        ),
        "learning_rate": trial.suggest_float(
            "learning_rate",
            TRAINING_PARAMS["learning_rate_min"],
            TRAINING_PARAMS["learning_rate_max"],
            log=True,
        ),
        "subsample": trial.suggest_float("subsample", 0.5, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
        "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
        "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
        "scale_pos_weight": scale_pos_weight,
        "eval_metric": "auc",
        "tree_method": "hist",
        "random_state": 42,
        "n_jobs": -1,
    }
    model = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("clf", XGBClassifier(**params)),
        ]
    )
    model.fit(
        X_tr,
        y_tr,
        clf__eval_set=[(X_va, y_va)],
        clf__verbose=False,
    )
    proba = model.predict_proba(X_va)[:, 1]
    return roc_auc_score(y_va, proba)


def train():
    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    mlflow.set_experiment(cfg.mlflow_experiment_name)
    os.environ["AWS_ACCESS_KEY_ID"] = cfg.seaweed_access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = cfg.seaweed_secret_key
    mlflow.set_registry_uri(cfg.mlflow_tracking_uri)

    print("Loading data …")
    df = load_lake_training_data()
    print(f"Training rows loaded: {len(df):,}")
    df = engineer_features(df)

    if "label" not in df.columns:
        raise ValueError("Training data must contain a 'label' column")

    X = get_model_input_frame(df).values
    y = df["label"].to_numpy()
    if np.unique(y).size < 2:
        raise ValueError(
            "Training data contains a single class only. "
            "Seed labeled data first (scripts/ingestion_ctl.py seed) or ingest labeled feedback."
        )

    X_tr, X_va, y_tr, y_va = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )

    with mlflow.start_run() as run:
        print("Optimising hyperparameters (Optuna) …")
        study = optuna.create_study(
            direction="maximize", pruner=optuna.pruners.MedianPruner()
        )
        study.optimize(
            lambda t: objective(t, X_tr, y_tr, X_va, y_va),
            n_trials=TRAINING_PARAMS["n_trials"],
            show_progress_bar=False,
        )

        best_params = study.best_params
        pos = int((y_tr == 1).sum())
        neg = int((y_tr == 0).sum())
        best_params["scale_pos_weight"] = (neg / pos) if pos else 1.0
        mlflow.log_params(best_params)
        mlflow.log_param("n_features", int(X.shape[1]))

        final_model = Pipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "clf",
                    XGBClassifier(
                        **best_params, eval_metric="auc", random_state=42, n_jobs=-1
                    ),
                ),
            ]
        )
        final_model.fit(
            X_tr,
            y_tr,
            clf__eval_set=[(X_va, y_va)],
            clf__verbose=False,
        )
        proba = final_model.predict_proba(X_va)[:, 1]
        pred = (proba >= 0.5).astype(int)

        metrics = {
            "auc_roc": roc_auc_score(y_va, proba),
            "f1": f1_score(y_va, pred),
            "avg_prec": average_precision_score(y_va, proba),
        }
        mlflow.log_metrics(metrics)
        Path("metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        print("Metrics:", metrics)

        mlflow.sklearn.log_model(
            final_model,
            artifact_path="model",
            registered_model_name=cfg.model_name,
        )

        # Promote the newest registered version to the target stage used by serving.
        client = MlflowClient(cfg.mlflow_tracking_uri)
        latest = client.get_latest_versions(cfg.model_name)
        if latest:
            newest = max(latest, key=lambda v: int(v.version))
            client.transition_model_version_stage(
                name=cfg.model_name,
                version=newest.version,
                stage=cfg.model_stage,
                archive_existing_versions=True,
            )
            print(
                f"Promoted model '{cfg.model_name}' version {newest.version} "
                f"to stage '{cfg.model_stage}'"
            )

        print(f"Run ID: {run.info.run_id}")
        return run.info.run_id


if __name__ == "__main__":
    run_id = train()
    sys.exit(0)
