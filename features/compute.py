"""Batch computation of user and merchant velocity features.

Reads the raw lake, aggregates amount activity per entity per hour, and writes
two fixed parquet keys that Feast serves (feature_repo/definitions.py):

    s3://<bucket>/features/user_stats.parquet
    s3://<bucket>/features/merchant_stats.parquet

Leakage rule: every aggregate row is stamped with the END of its hour bucket, so
a transaction scored at time t can only ever see aggregates from hours that
closed before t — never its own hour. Feast's 7-day TTL then supplies the
trailing window at retrieval time.

Both files are overwritten on every run: they are derived state, regenerated
from the lake, not accumulated.
"""

from __future__ import annotations

import logging

import pandas as pd

from config.settings import get_settings
from datalake.storage import list_parquet, read_parquet_keys, write_parquet

log = logging.getLogger("features")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

MIN_ROWS = 1000

_USER_AGGS = {
    "user_txn_count_7d": "count",
    "user_amount_sum_7d": "sum",
    "user_amount_avg_7d": "mean",
    "user_amount_max_7d": "max",
}
_MERCHANT_AGGS = {
    "merchant_txn_count_7d": "count",
    "merchant_amount_avg_7d": "mean",
}


def _hour_bucket_ends(df: pd.DataFrame) -> pd.Series:
    """Timestamp of the hour-bucket END each row belongs to (tz-aware UTC)."""
    return pd.to_datetime(df["timestamp"], utc=True).dt.floor("h") + pd.Timedelta(hours=1)


def compute_velocity_features(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (user_stats, merchant_stats) as per-entity, per-hour aggregates."""
    work = df.assign(
        event_timestamp=_hour_bucket_ends(df),
        amount_usd=df["amount_usd"].astype(float),
    )

    user_stats = (
        work.groupby(["user_id", "event_timestamp"])["amount_usd"].agg(**_USER_AGGS).reset_index()
    )
    merchant_stats = (
        work.groupby(["merchant_id", "event_timestamp"])["amount_usd"]
        .agg(**_MERCHANT_AGGS)
        .reset_index()
    )

    for frame, columns in (
        (user_stats, list(_USER_AGGS)),
        (merchant_stats, list(_MERCHANT_AGGS)),
    ):
        frame[columns] = frame[columns].astype("float32")

    return user_stats, merchant_stats


def main() -> None:
    cfg = get_settings()
    keys = list_parquet("raw/")
    df = read_parquet_keys(keys) if keys else pd.DataFrame()

    if len(df) < MIN_ROWS:
        raise SystemExit(
            f"not enough lake rows to compute features: {len(df)} "
            f"(need {MIN_ROWS}; run `make seed` to bootstrap the lake)"
        )

    user_stats, merchant_stats = compute_velocity_features(df)
    user_path = write_parquet(user_stats, f"{cfg.feature_stats_prefix}/user_stats.parquet")
    merchant_path = write_parquet(
        merchant_stats, f"{cfg.feature_stats_prefix}/merchant_stats.parquet"
    )

    log.info(
        "Wrote %d user rows (%d users) → %s",
        len(user_stats),
        user_stats["user_id"].nunique(),
        user_path,
    )
    log.info(
        "Wrote %d merchant rows (%d merchants) → %s",
        len(merchant_stats),
        merchant_stats["merchant_id"].nunique(),
        merchant_path,
    )


if __name__ == "__main__":
    main()
