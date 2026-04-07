"""The Great Expectations suite must pass good data and name bad columns."""

from __future__ import annotations

import pandas as pd

from gx.validate import validate_frame


def _frame(n_rows: int = 600) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "transaction_id": [f"t{i}" for i in range(n_rows)],
            "amount_usd": [10.0] * n_rows,
            "label": [0] * (n_rows - 10) + [1] * 10,
            "device_type": ["mobile"] * n_rows,
        }
    )


def test_valid_frame_passes():
    passed, failures = validate_frame(_frame())
    assert passed, failures
    assert failures == []


def test_out_of_range_amount_fails_with_named_column():
    df = _frame()
    df.loc[0, "amount_usd"] = 0.0
    passed, failures = validate_frame(df)
    assert not passed
    assert "expect_column_values_to_be_between:amount_usd" in failures
