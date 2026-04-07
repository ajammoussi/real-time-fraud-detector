"""The shared scoring path: threshold from the model, real inputs, no fallbacks."""

from __future__ import annotations

import numpy as np
import pytest

import api.inference as inference
from features.schema import VELOCITY_FEATURES
from training.feature_engineering import FEATURE_COLS

TRANSACTION = {
    "transaction_id": "txn_score_1",
    "timestamp": "2026-04-07T10:00:00Z",
    "user_id": "usr_00001",
    "merchant_id": "mrch_0001",
    "merchant_cat": "electronics",
    "amount_usd": 1500.0,
    "currency": "USDT",
    "country": "ZZ",
    "device_type": "exchange_taker",
    "ip_hash": "aabb1122",
    "card_last4": "1234",
    "is_international": False,
    "hour_of_day": 10,
    "day_of_week": 1,
}


class _StubModel:
    def __init__(self, probability: float = 0.9, explode: bool = False):
        self.probability = probability
        self.explode = explode
        self.seen_shape: tuple[int, ...] | None = None

    def predict_proba(self, values):
        self.seen_shape = values.shape
        if self.explode:
            raise RuntimeError("model exploded")
        return np.array([[1 - self.probability, self.probability]])


@pytest.fixture(autouse=True)
def _no_feature_store(monkeypatch):
    """Cold feature store: the scorer must fall back to default velocity values."""
    monkeypatch.setattr(
        "features.store.online_features",
        lambda users, merchants: (_ for _ in ()).throw(RuntimeError("redis down")),
    )


def test_decision_uses_model_threshold():
    model = _StubModel(probability=0.9)
    result = inference.score(
        TRANSACTION,
        model=model,
        info={"version": "3", "decision_threshold": 0.8},
    )

    assert result["decision"] == "REJECT"
    assert result["model_version"] == "3"
    assert result["threshold"] == 0.8
    assert model.seen_shape == (1, len(FEATURE_COLS))
    assert set(result["model_features"]) == set(FEATURE_COLS)
    assert result["model_features"][VELOCITY_FEATURES[0]] == 0.0


def test_approve_below_threshold():
    result = inference.score(
        TRANSACTION,
        model=_StubModel(probability=0.1),
        info={"version": "3", "decision_threshold": 0.5},
    )
    assert result["decision"] == "APPROVE"


def test_model_errors_propagate():
    with pytest.raises(RuntimeError, match="model exploded"):
        inference.score(
            TRANSACTION,
            model=_StubModel(explode=True),
            info={"version": "3", "decision_threshold": 0.5},
        )
