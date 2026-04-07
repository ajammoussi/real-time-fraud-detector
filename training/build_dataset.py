"""Build the training dataset: lake → validated → featured → parquet.

Stages:
  1. Read the newest raw parquet files from the lake (bounded by max_files).
  2. Sample deterministically and keep only labelled rows.
  3. Enforce the dataset contract with Great Expectations — a failure blocks
     training here, which is the authoritative gate (the lake consumer only
     runs cheap pre-checks).
  4. Join point-in-time velocity features from Feast and engineer the
     row-level features.
  5. Write data/dataset.parquet.

Labels only ever exist for rows produced by the synthetic seed path, so a lake
full of live (unlabelled) trades yields no training data by design.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

import pandas as pd

from config.params import get_params
from datalake.storage import SEED_PREFIX, list_parquet, read_parquet_keys
from features import store as feature_store
from gx.validate import validate_frame
from training.feature_engineering import engineer_features

log = logging.getLogger("build_dataset")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

DATASET_PATH = Path(__file__).resolve().parents[1] / "data" / "dataset.parquet"


def _load_from_lake() -> pd.DataFrame:
    params = get_params()
    # Labels only enter the lake through the seed path (README "Dataset &
    # labels"); live Binance batches are unlabelled by design. Taking the
    # newest N files blindly would drop the only labelled batch out of the
    # window as soon as the producer starts writing.
    keys = list_parquet(SEED_PREFIX)
    if not keys:
        log.warning(
            "no labelled seed batch in the lake — falling back to raw/; "
            "training will fail unless those batches carry labels"
        )
        keys = list_parquet("raw/")
    max_files = params.dataset.max_files
    if max_files > 0:
        keys = keys[-max_files:]
    return read_parquet_keys(keys)


def build_dataset(
    load_raw: Callable[[], pd.DataFrame] | None = None,
) -> Path:
    """Assemble and persist the training dataset; returns the parquet path."""
    params = get_params()
    df = (load_raw or _load_from_lake)()

    max_rows = params.dataset.max_rows
    if max_rows > 0 and len(df) > max_rows:
        df = df.sample(n=max_rows, random_state=42).reset_index(drop=True)

    if "label" not in df.columns:
        raise ValueError(
            "lake has no 'label' column — the live feed is unlabelled; "
            "run `make seed` to bootstrap labelled rows"
        )
    df = df[df["label"].notna()].reset_index(drop=True)

    n_labeled = len(df)
    n_fraud = int(df["label"].sum())
    min_labeled = params.dataset.min_labeled_rows
    if n_labeled < min_labeled or df["label"].nunique() < 2:
        raise ValueError(
            f"not enough labelled training data: {n_labeled} labelled rows "
            f"({n_fraud} fraud) — dataset.min_labeled_rows={min_labeled} and both "
            "classes are required; run `make seed` to bootstrap labelled rows"
        )

    passed, failures = validate_frame(df)
    if not passed:
        print("Dataset contract violated:", failures)
        raise SystemExit(1)

    log.info(
        "Dataset contract passed: %d labelled rows, %.2f%% fraud",
        n_labeled,
        100 * n_fraud / n_labeled,
    )

    entity_df = df[["transaction_id", "user_id", "merchant_id"]].assign(
        event_timestamp=pd.to_datetime(df["timestamp"], utc=True)
    )
    with_features = df.merge(
        feature_store.historical_features(entity_df),
        on="transaction_id",
        how="left",
    ).reset_index(drop=True)

    dataset = feature_store.with_defaults(engineer_features(with_features))

    DATASET_PATH.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_parquet(DATASET_PATH, index=False)

    log.info(
        "Wrote %s: %d rows, %d columns, %.2f%% fraud",
        DATASET_PATH,
        len(dataset),
        len(dataset.columns),
        100 * float(dataset["label"].mean()),
    )
    return DATASET_PATH


if __name__ == "__main__":
    build_dataset()
