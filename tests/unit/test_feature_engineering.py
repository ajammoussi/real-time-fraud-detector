"""Unit tests for feature engineering."""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import numpy as np
import pytest
from training.feature_engineering import engineer_features


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
