"""Model quality gates — run after training."""

from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from mlflow.exceptions import MlflowException
from sklearn.metrics import f1_score, roc_auc_score

from api.model_loader import load_model
from config.settings import get_settings
from training.feature_engineering import engineer_features

cfg = get_settings()


@pytest.fixture(scope="module")
def val_data():
    raw_dir = Path("data/raw")
    parquet_files = sorted(raw_dir.glob("*.parquet"))
    if not parquet_files:
        pytest.skip("No local parquet files in data/raw for model quality tests")

    df = pd.concat((pd.read_parquet(p) for p in parquet_files[:3]), ignore_index=True)
    if "label" not in df.columns:
        pytest.skip("Local parquet data is missing label column")

    df_feat = engineer_features(df)
    avail_cols = [c for c in df_feat.columns if c != "label"]
    X = df_feat[avail_cols].fillna(0).values
    y = df["label"].values
    return X, y


@pytest.fixture(scope="module")
def model():
    try:
        return load_model("Staging")
    except MlflowException:
        pytest.skip("No model registered in MLflow 'Staging' for model quality tests")


def test_auc_roc_above_threshold(model, val_data):
    X, y = val_data
    proba = model.predict_proba(X)[:, 1]
    auc = roc_auc_score(y, proba)
    assert auc >= 0.92, f"AUC {auc:.4f} < 0.92"


def test_f1_above_threshold(model, val_data):
    X, y = val_data
    pred = model.predict(X)
    f1 = f1_score(y, pred)
    assert f1 >= 0.70, f"F1 {f1:.4f} < 0.70"


def test_inference_latency_p99(model, val_data):
    X, _ = val_data
    latencies = []
    for row in X[:200]:
        t0 = time.perf_counter()
        model.predict_proba(row.reshape(1, -1))
        latencies.append((time.perf_counter() - t0) * 1000)
    p99 = np.percentile(latencies, 99)
    assert p99 < 50.0, f"p99 latency {p99:.1f}ms >= 50ms"


def test_no_degenerate_predictions(model, val_data):
    X, _ = val_data
    proba = model.predict_proba(X)[:, 1]
    assert proba.mean() > 0.001, "Mean predicted fraud prob is suspiciously low"
    assert proba.mean() < 0.5, "Mean predicted fraud prob is suspiciously high"
