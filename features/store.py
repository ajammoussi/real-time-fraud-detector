"""The only place Feast is touched at runtime.

Training (historical retrieval) and serving (online lookup) both come through
here, so the feature names and the "cold entity" fallback are defined once.
`get_store()` is not constructed at import time: callers catch failures and
degrade explicitly (a missing feature store must never break scoring outright).
"""

from __future__ import annotations

from functools import lru_cache

import pandas as pd
from feast import FeatureStore

from config.settings import get_settings
from features.schema import (
    MERCHANT_FEATURES,
    USER_FEATURES,
    VELOCITY_FEATURES,
)

_FEATURE_REFS = [f"user_stats:{f}" for f in USER_FEATURES] + [
    f"merchant_stats:{f}" for f in MERCHANT_FEATURES
]


@lru_cache
def get_store() -> FeatureStore:
    """Return the Feast store for the configured repo path (cached)."""
    return FeatureStore(repo_path=get_settings().feature_repo_path)


def historical_features(entity_df: pd.DataFrame) -> pd.DataFrame:
    """Retrieve point-in-time velocity features for *entity_df*.

    *entity_df* must carry transaction_id, user_id, merchant_id and
    event_timestamp. Returns transaction_id plus every VELOCITY_FEATURES column,
    in the same row order as the input.
    """
    retrieved = (
        get_store().get_historical_features(entity_df=entity_df, features=_FEATURE_REFS).to_df()
    )

    if len(retrieved) != len(entity_df):
        raise ValueError(
            f"feature store returned {len(retrieved)} rows for {len(entity_df)} entities"
        )

    missing = [c for c in ["transaction_id", *VELOCITY_FEATURES] if c not in retrieved]
    if missing:
        raise ValueError(f"feature store returned no columns: {missing}")

    return entity_df[["transaction_id"]].merge(
        retrieved[["transaction_id", *VELOCITY_FEATURES]], on="transaction_id"
    )


def online_features(user_ids: list[str], merchant_ids: list[str]) -> pd.DataFrame:
    """Online lookup for one row per (user_id, merchant_id) pair.

    Cold entities come back as None — the caller runs `with_defaults`.
    """
    rows = [
        {"user_id": user, "merchant_id": merchant}
        for user, merchant in zip(user_ids, merchant_ids, strict=True)
    ]
    if not rows:
        return pd.DataFrame(columns=VELOCITY_FEATURES)

    result = get_store().get_online_features(features=_FEATURE_REFS, entity_rows=rows).to_df()
    return result[VELOCITY_FEATURES]


def with_defaults(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing/cold velocity features with 0.0, adding absent columns."""
    out = df.copy()
    for column in VELOCITY_FEATURES:
        if column not in out.columns:
            out[column] = 0.0
    out[VELOCITY_FEATURES] = (
        out[VELOCITY_FEATURES].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    )
    return out
