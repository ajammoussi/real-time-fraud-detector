"""The single scoring path shared by the HTTP API and the Kafka consumer.

Both entry points must score identically — same feature engineering, same
feature-store fallback, same threshold — otherwise "shadow" comparisons and
audit rows describe different models.
"""

from __future__ import annotations

import logging

import pandas as pd

from api.metrics import feature_store_errors_total
from api.model_loader import load_model_and_info
from config.settings import get_settings
from features import store as feature_store
from features.schema import VELOCITY_FEATURES
from training.feature_engineering import engineer_features, get_model_input_frame

log = logging.getLogger("api.inference")

_feature_store_warned = False


def _velocity_features(user_id: str, merchant_id: str) -> pd.DataFrame:
    """Online velocity features, degrading to defaults if Feast is unreachable."""
    global _feature_store_warned
    try:
        return feature_store.online_features([user_id], [merchant_id])
    except Exception as exc:
        feature_store_errors_total.inc()
        if not _feature_store_warned:
            log.warning(
                "feature store unavailable — scoring with default velocity features: %s",
                exc,
            )
            _feature_store_warned = True
        return pd.DataFrame(columns=VELOCITY_FEATURES)


def score(transaction: dict, *, model=None, info: dict | None = None) -> dict:
    """Score one transaction and return the probability, decision and inputs."""
    cfg = get_settings()
    if model is None or info is None:
        # One lookup so the version and threshold always describe the artifact
        # in memory, never a version that has just been promoted ahead of it.
        loaded_model, loaded_info = load_model_and_info()
        if model is None:
            model = loaded_model
        if info is None:
            info = loaded_info

    # user/merchant ids are needed for the feature lookup and then dropped by
    # engineer_features, so read them from the raw transaction first.
    velocity = _velocity_features(transaction["user_id"], transaction["merchant_id"])

    frame = engineer_features(pd.DataFrame([transaction])).reset_index(drop=True)
    frame = pd.concat([frame, velocity.reset_index(drop=True)], axis=1)
    model_input = get_model_input_frame(feature_store.with_defaults(frame))

    fraud_probability = float(model.predict_proba(model_input.values)[0, 1])
    threshold = float(info.get("decision_threshold", cfg.decision_threshold))

    return {
        "fraud_probability": fraud_probability,
        "decision": "REJECT" if fraud_probability >= threshold else "APPROVE",
        "model_version": str(info.get("version", "unknown")),
        "threshold": threshold,
        "model_features": {name: float(value) for name, value in model_input.iloc[0].items()},
    }
