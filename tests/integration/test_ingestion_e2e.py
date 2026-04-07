"""Offline round-trip: producer mapping → lake pre-checks → model input.

No Kafka, no S3, no database — this exercises the schema contract that ties the
three stages together, which is where the pipeline actually breaks.
"""

from __future__ import annotations

import pandas as pd

from features.schema import VELOCITY_FEATURES
from ingestion.lake_consumer import _validate_batch
from ingestion.producer import _map_binance_trade
from training.feature_engineering import (
    FEATURE_COLS,
    engineer_features,
    get_model_input_frame,
)

AGG_TRADE = {
    "e": "aggTrade",
    "E": 1775563200000,
    "a": 12345,
    "s": "BTCUSDT",
    "p": "65000.10",
    "q": "0.0021",
    "f": 1,
    "l": 2,
    "T": 1775563200123,
    "m": True,
}


def test_trade_round_trip_to_model_input(tmp_path):
    record = _map_binance_trade(AGG_TRADE)
    assert "label" not in record  # a public trade feed is unlabelled

    passed, failures = _validate_batch(pd.DataFrame([record]))
    assert passed, failures
    assert failures == []

    # What the lake writer persists: provenance columns stripped.
    lake_row = {k: v for k, v in record.items() if not k.startswith("_")}
    path = tmp_path / "batch.parquet"
    pd.DataFrame([lake_row]).to_parquet(path, index=False)

    frame = pd.read_parquet(path)
    frame = engineer_features(frame)
    frame = frame.assign(**{column: 0.0 for column in VELOCITY_FEATURES})

    inputs = get_model_input_frame(frame)
    assert list(inputs.columns) == FEATURE_COLS


def test_out_of_range_amount_is_rejected_before_the_lake():
    record = _map_binance_trade(AGG_TRADE)
    record["amount_usd"] = 0.0

    passed, failures = _validate_batch(pd.DataFrame([record]))

    assert not passed
    assert failures == ["amount_out_of_range:1_rows"]
