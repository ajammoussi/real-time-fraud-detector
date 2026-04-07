"""The dataset stage must enforce the labelled-data contract before training."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import training.build_dataset as build_dataset_module
from features.schema import VELOCITY_FEATURES
from training.feature_engineering import FEATURE_COLS


def _raw_frame(n_rows: int) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    timestamps = pd.date_range("2026-04-01", periods=n_rows, freq="min", tz="UTC")
    return pd.DataFrame(
        {
            "transaction_id": [f"txn_{i}" for i in range(n_rows)],
            "timestamp": [ts.isoformat() for ts in timestamps],
            "user_id": [f"usr_{i % 50:05d}" for i in range(n_rows)],
            "merchant_id": [f"mrch_{i % 20:04d}" for i in range(n_rows)],
            "merchant_cat": ["electronics"] * n_rows,
            "amount_usd": rng.uniform(1, 500, n_rows).round(2),
            "currency": ["USDT"] * n_rows,
            "country": ["ZZ"] * n_rows,
            "device_type": ["exchange_taker"] * n_rows,
            "ip_hash": ["aabb1122"] * n_rows,
            "card_last4": ["1234"] * n_rows,
            "is_international": [False] * n_rows,
            "hour_of_day": [ts.hour for ts in timestamps],
            "day_of_week": [ts.weekday() for ts in timestamps],
            "label": [1 if i % 20 == 0 else 0 for i in range(n_rows)],
        }
    )


def _stub_historical_features(entity_df: pd.DataFrame) -> pd.DataFrame:
    return entity_df[["transaction_id"]].assign(**{column: 0.0 for column in VELOCITY_FEATURES})


def test_build_dataset_writes_all_features(monkeypatch, tmp_path):
    monkeypatch.setattr(
        build_dataset_module.feature_store,
        "historical_features",
        _stub_historical_features,
    )
    output = tmp_path / "dataset.parquet"
    monkeypatch.setattr(build_dataset_module, "DATASET_PATH", output)

    path = build_dataset_module.build_dataset(load_raw=lambda: _raw_frame(2000))

    dataset = pd.read_parquet(path)
    assert path == output
    assert len(dataset) == 2000
    assert all(column in dataset.columns for column in FEATURE_COLS)
    assert "label" in dataset.columns


def test_build_dataset_rejects_unlabelled_lake(monkeypatch, tmp_path):
    monkeypatch.setattr(build_dataset_module, "DATASET_PATH", tmp_path / "d.parquet")
    with pytest.raises(ValueError, match="min_labeled_rows"):
        build_dataset_module.build_dataset(load_raw=lambda: _raw_frame(10))
