"""Unit tests for feature engineering."""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

from features.schema import VELOCITY_FEATURES
from training.feature_engineering import (
    FEATURE_COLS,
    engineer_features,
    get_model_input_frame,
)


@pytest.fixture
def sample_df():
    rows = []
    now = datetime.now(timezone.utc).isoformat()
    for i in range(200):
        rows.append(
            {
                "transaction_id": f"txn_test_{i}",
                "timestamp": now,
                "user_id": f"usr_{i % 20:05d}",
                "merchant_id": f"mrch_{i % 10:04d}",
                "merchant_cat": "crypto_exchange" if i % 2 == 0 else "electronics",
                "amount_usd": float(10 + i),
                "currency": "USD",
                "country": "US",
                "device_type": "exchange_taker" if i % 3 else "exchange_maker",
                "ip_hash": f"{i:08x}",
                "card_last4": f"{1000 + (i % 9000)}",
                "is_international": bool(i % 5 == 0),
                "hour_of_day": i % 24,
                "day_of_week": i % 7,
                "label": 1 if i % 20 == 0 else 0,
            }
        )
    return pd.DataFrame(rows)


def test_no_transaction_id_leakage(sample_df):
    out = engineer_features(sample_df)
    assert "transaction_id" not in out.columns


def test_amount_log_positive(sample_df):
    out = engineer_features(sample_df)
    assert (out["amount_log"] >= 0).all()


def test_no_inf_values(sample_df):
    out = engineer_features(sample_df)
    numeric = out.select_dtypes(include=[np.number])
    assert not np.isinf(numeric.values).any()


def test_country_not_a_feature(sample_df):
    """`country` comes from the quote asset, not real geography — never a feature."""
    out = engineer_features(sample_df)
    assert "country" not in out.columns
    assert "country" not in FEATURE_COLS


def test_velocity_features_required(sample_df):
    """A missing velocity feature must fail loudly, not silently narrow the input."""
    out = engineer_features(sample_df)
    with pytest.raises(ValueError, match="missing model features"):
        get_model_input_frame(out)

    complete = out.assign(**{column: 0.0 for column in VELOCITY_FEATURES})
    frame = get_model_input_frame(complete)
    assert list(frame.columns) == FEATURE_COLS
