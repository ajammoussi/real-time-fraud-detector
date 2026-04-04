"""Feature retrieval from Feast + manual feature construction."""
from __future__ import annotations
import hashlib
import pandas as pd
import numpy as np
from feast import FeatureStore


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


def _encode_with_map(series: pd.Series, mapping: dict[str, int], uppercase: bool = False) -> pd.Series:
    normalised = series.fillna("").astype(str).str.upper() if uppercase else series.fillna("").astype(str).str.lower()
    return normalised.map(mapping).fillna(len(mapping)).astype(int)


def get_feast_features(df: pd.DataFrame, repo_path: str = "./feast") -> pd.DataFrame:
    store = FeatureStore(repo_path=repo_path)
    entity_df = df[["user_id", "merchant_id", "timestamp"]].copy()
    entity_df["event_timestamp"] = pd.to_datetime(entity_df["timestamp"], utc=True)

    feast_df = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "user_features:amount_usd",
            "user_features:hour_of_day",
            "user_features:day_of_week",
            "user_features:is_international",
            "merchant_features:amount_usd",
            "merchant_features:merchant_cat",
        ],
    ).to_df()
    return feast_df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Stateless, row-level feature engineering."""
    out = df.copy()
    out["amount_log"]         = np.log1p(out["amount_usd"])
    out["is_high_amount"]     = (out["amount_usd"] > 1000).astype(int)
    out["is_odd_hour"]        = (out["hour_of_day"].between(0, 5)).astype(int)
    out["is_weekend"]         = (out["day_of_week"] >= 5).astype(int)
    out["is_international"]   = out["is_international"].astype(int)

    out["merchant_cat"] = _encode_with_map(out["merchant_cat"], _MERCHANT_CAT_MAP)
    out["device_type"] = _encode_with_map(out["device_type"], _DEVICE_TYPE_MAP)
    out["currency"] = _encode_with_map(out["currency"], _CURRENCY_MAP, uppercase=True)
    # Hash country codes into a deterministic small integer space.
    out["country"] = (
        out["country"]
        .fillna("ZZ")
        .astype(str)
        .str.upper()
        .map(lambda c: int(hashlib.md5(c.encode("utf-8")).hexdigest()[:8], 16) % 997)
    ).astype(int)

    drop_cols = ["transaction_id", "timestamp", "user_id", "merchant_id",
                 "ip_hash", "card_last4"]
    out = out.drop(columns=[c for c in drop_cols if c in out.columns])
    return out


FEATURE_COLS = [
    "amount_usd", "amount_log", "is_high_amount", "is_odd_hour", "is_weekend",
    "is_international", "hour_of_day", "day_of_week",
    "merchant_cat", "device_type", "currency", "country",
]


def get_model_input_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Return a deterministic model input frame ordered by FEATURE_COLS.

    This keeps training and inference column ordering aligned even when
    some optional Feast features are not present in the data.
    """
    ordered_cols = [c for c in FEATURE_COLS if c in df.columns]
    if not ordered_cols:
        raise ValueError("No model feature columns were found after feature engineering")
    return df[ordered_cols].fillna(0)
