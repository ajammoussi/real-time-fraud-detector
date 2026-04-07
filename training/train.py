"""Training entrypoint: dataset → Optuna HPO → quality gate → MLflow registry.

Splits are 60/20/20 (train/valid/test): HPO and the decision threshold are
chosen on `valid`, and the reported metrics come from the untouched `test` split.
The classifier is a bare XGBClassifier — gradient-boosted trees do not need
feature scaling, so wrapping it in a StandardScaler Pipeline only added skew
between training and serving.

The quality gate runs BEFORE the model is registered: if the new model does not
beat params.yaml's min_auc / min_avg_prec, nothing is registered and the process
exits non-zero. Promotion to a stage is scripts/model_ctl.py's job.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import mlflow
import numpy as np
import optuna
import pandas as pd
from mlflow.tracking import MlflowClient
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

from config.params import get_params
from config.settings import get_settings
from training.feature_engineering import get_model_input_frame

cfg = get_settings()
optuna.logging.set_verbosity(optuna.logging.WARNING)

_ROOT_DIR = Path(__file__).resolve().parents[1]
DATASET_PATH = _ROOT_DIR / "data" / "dataset.parquet"
METRICS_PATH = _ROOT_DIR / "metrics.json"


def load_dataset() -> pd.DataFrame:
    if not DATASET_PATH.exists():
        raise FileNotFoundError(f"{DATASET_PATH} not found — run `make data` first")
    return pd.read_parquet(DATASET_PATH)


def _scale_pos_weight(y: np.ndarray) -> float:
    positives = int((y == 1).sum())
    negatives = int((y == 0).sum())
    return (negatives / positives) if positives else 1.0


def _make_classifier(trial_params: dict, scale_pos_weight: float) -> XGBClassifier:
    params = get_params()
    return XGBClassifier(
        **trial_params,
        scale_pos_weight=scale_pos_weight,
        eval_metric="aucpr",
        early_stopping_rounds=params.model.early_stopping_rounds,
        tree_method="hist",
        random_state=42,
        n_jobs=-1,
    )


def objective(trial: optuna.Trial, X_tr, y_tr, X_va, y_va) -> float:
    """PR-AUC on the validation split — the right objective at a ~2% base rate."""
    params = get_params()
    trial_params = {
        "n_estimators": trial.suggest_int(
            "n_estimators",
            params.model.n_estimators_min,
            params.model.n_estimators_max,
            step=50,
        ),
        "max_depth": trial.suggest_int("max_depth", 3, params.model.max_depth_max),
        "learning_rate": trial.suggest_float(
            "learning_rate",
            params.model.learning_rate_min,
            params.model.learning_rate_max,
            log=True,
        ),
        "subsample": trial.suggest_float("subsample", 0.5, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
        "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
        "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
    }
    model = _make_classifier(trial_params, _scale_pos_weight(y_tr))
    model.fit(X_tr, y_tr, eval_set=[(X_va, y_va)], verbose=False)
    return average_precision_score(y_va, model.predict_proba(X_va)[:, 1])


def choose_threshold(y_valid: np.ndarray, proba_valid: np.ndarray) -> float:
    """Pick the F1-maximising threshold on the validation split.

    Ties resolve to the smallest threshold (np.argmax returns the first maximum
    of an ascending scan).
    """
    thresholds = np.linspace(0.05, 0.95, 91)
    scores = [
        f1_score(y_valid, proba_valid >= threshold, zero_division=0) for threshold in thresholds
    ]
    return float(thresholds[int(np.argmax(scores))])


def _write_metrics(payload: dict) -> None:
    METRICS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def train() -> str:
    params = get_params()
    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    mlflow.set_experiment(cfg.mlflow_experiment_name)
    mlflow.set_registry_uri(cfg.mlflow_tracking_uri)
    os.environ["AWS_ACCESS_KEY_ID"] = cfg.seaweed_access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = cfg.seaweed_secret_key
    os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", cfg.mlflow_s3_endpoint_url)

    df = load_dataset()
    X = get_model_input_frame(df).values
    y = df["label"].to_numpy()
    print(f"Training rows: {len(df):,} | features: {X.shape[1]} | fraud: {y.mean():.2%}")

    X_train, X_rest, y_train, y_rest = train_test_split(
        X, y, test_size=0.4, stratify=y, random_state=42
    )
    X_valid, X_test, y_valid, y_test = train_test_split(
        X_rest, y_rest, test_size=0.5, stratify=y_rest, random_state=42
    )

    with mlflow.start_run() as run:
        print(f"Optimising hyperparameters ({params.model.n_trials} trials) …")
        study = optuna.create_study(direction="maximize")
        study.optimize(
            lambda trial: objective(trial, X_train, y_train, X_valid, y_valid),
            n_trials=params.model.n_trials,
            show_progress_bar=False,
        )

        best_params = dict(study.best_params)
        scale_pos_weight = _scale_pos_weight(y_train)
        final_model = _make_classifier(best_params, scale_pos_weight)
        final_model.fit(X_train, y_train, eval_set=[(X_valid, y_valid)], verbose=False)

        threshold = choose_threshold(y_valid, final_model.predict_proba(X_valid)[:, 1])
        proba_test = final_model.predict_proba(X_test)[:, 1]
        predicted = proba_test >= threshold
        metrics = {
            "auc_roc": float(roc_auc_score(y_test, proba_test)),
            "avg_prec": float(average_precision_score(y_test, proba_test)),
            "f1_at_threshold": float(f1_score(y_test, predicted, zero_division=0)),
            "precision": float(precision_score(y_test, predicted, zero_division=0)),
            "recall": float(recall_score(y_test, predicted, zero_division=0)),
        }

        mlflow.log_params({**best_params, "scale_pos_weight": scale_pos_weight})
        mlflow.log_param("n_features", int(X.shape[1]))
        mlflow.log_param("decision_threshold", threshold)
        mlflow.log_metrics(metrics)

        report = {
            **metrics,
            "threshold": threshold,
            "run_id": run.info.run_id,
            "model_version": None,
            "n_train": len(X_train),
            "n_valid": len(X_valid),
            "n_test": len(X_test),
            "label_rate": float(y.mean()),
            "n_features": int(X.shape[1]),
        }

        if (
            metrics["auc_roc"] < params.model.min_auc
            or metrics["avg_prec"] < params.model.min_avg_prec
        ):
            _write_metrics(report)
            print(
                "QUALITY GATE FAILED: "
                f"auc_roc={metrics['auc_roc']:.4f} (min {params.model.min_auc}), "
                f"avg_prec={metrics['avg_prec']:.4f} "
                f"(min {params.model.min_avg_prec}) — nothing registered"
            )
            raise SystemExit(1)

        # XGBoost flavor: skops (mlflow.sklearn's default serializer) refuses
        # XGBClassifier as an untrusted type, and the native UBJ format is the
        # right artifact for an XGBClassifier anyway.
        info = mlflow.xgboost.log_model(
            final_model,
            name="model",
            registered_model_name=cfg.model_name,
        )
        MlflowClient(cfg.mlflow_tracking_uri).set_model_version_tag(
            cfg.model_name,
            info.registered_model_version,
            "decision_threshold",
            str(threshold),
        )
        report["model_version"] = info.registered_model_version
        _write_metrics(report)

        print(
            f"Registered {cfg.model_name} v{info.registered_model_version} "
            f"(threshold {threshold:.2f}) for run {run.info.run_id}"
        )
        return run.info.run_id


if __name__ == "__main__":
    print(train())
