"""Row-level feature engineering and the model input contract.

`engineer_features` derives stateless per-row features. The velocity features
(user/merchant 7-day aggregates) are *not* derived here: they come from Feast
(features.store), because they need point-in-time history. `FEATURE_COLS` is the
single ordered contract both training and serving build their input from.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from features.schema import VELOCITY_FEATURES

_MERCHANT_CAT_MAP = {
    "crypto_exchange": 0,
    "electronics": 1,
    "grocery": 2,
    "travel": 3,
    "clothing": 4,
    "restaurants": 5,
    "gaming": 6,
    "healthcare": 7,
    "fuel": 8,
    "entertainment": 9,
    "utilities": 10,
    "finance": 11,
}

_DEVICE_TYPE_MAP = {
    "mobile": 0,
    "desktop": 1,
    "tablet": 2,
    "pos_terminal": 3,
    "exchange_maker": 4,
    "exchange_taker": 5,
}

_CURRENCY_MAP = {
    "USD": 0,
    "USDT": 1,
    "USDC": 2,
    "EUR": 3,
    "GBP": 4,
    "BTC": 5,
    "ETH": 6,
    "BNB": 7,
}

# Dropped before inference. `country` is included deliberately: it is derived
# from the quote asset in the feed, carries no real geography, and is therefore
# never a model feature.
_DROP_COLS = [
    "transaction_id",
    "timestamp",
    "user_id",
    "merchant_id",
    "ip_hash",
    "card_last4",
    "country",
]

FEATURE_COLS = [
    "amount_usd",
    "amount_log",
    "is_high_amount",
    "is_odd_hour",
    "is_weekend",
    "is_international",
    "hour_of_day",
    "day_of_week",
    "merchant_cat",
    "device_type",
    "currency",
    *VELOCITY_FEATURES,
]


def _encode_with_map(
    series: pd.Series, mapping: dict[str, int], uppercase: bool = False
) -> pd.Series:
    if uppercase:
        normalised = series.fillna("").astype(str).str.upper()
    else:
        normalised = series.fillna("").astype(str).str.lower()

    return normalised.map(mapping).fillna(len(mapping)).astype(int)


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Stateless, row-level feature engineering."""
    out = df.copy()
    out["amount_log"] = np.log1p(out["amount_usd"])
    out["is_high_amount"] = (out["amount_usd"] > 1000).astype(int)
    out["is_odd_hour"] = (out["hour_of_day"].between(0, 5)).astype(int)
    out["is_weekend"] = (out["day_of_week"] >= 5).astype(int)
    out["is_international"] = out["is_international"].astype(int)

    out["merchant_cat"] = _encode_with_map(out["merchant_cat"], _MERCHANT_CAT_MAP)
    out["device_type"] = _encode_with_map(out["device_type"], _DEVICE_TYPE_MAP)
    out["currency"] = _encode_with_map(out["currency"], _CURRENCY_MAP, uppercase=True)

    out = out.drop(columns=[c for c in _DROP_COLS if c in out.columns])
    return out


def get_model_input_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Return the model input frame, ordered by FEATURE_COLS.

    Raises instead of silently returning a shorter frame: a missing feature
    column means the model would be fed a differently-shaped vector at serving
    time, which must fail loudly here rather than as a shape error later.
    """
    missing = [c for c in FEATURE_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"missing model features: {missing}")
    return df[FEATURE_COLS].fillna(0)
