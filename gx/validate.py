"""Validate a transaction frame with Great Expectations.

The suite in gx/expectations/fraud_transactions_suite.json describes the
**training dataset contract**: the assembled, labelled frame that training
consumes. That is why `label` is required here and why the row-count minimum is
500 — an unlabelled lake batch would (correctly) fail this suite, while the
lake consumer's cheap pre-checks keep obviously broken rows out earlier.

CLI:
    python gx/validate.py <parquet path>     # local path or s3:// URI
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

# Great Expectations logs while it is still being imported, so the level has to
# be set before the import itself to keep CI logs readable.
logging.getLogger("great_expectations").setLevel(logging.WARNING)

import great_expectations as gx  # noqa: E402
import pandas as pd  # noqa: E402
from great_expectations import expectations as gx_expectations  # noqa: E402

from datalake.storage import read_parquet_keys  # noqa: E402

_SUITE_DIR = Path(__file__).parent / "expectations"

_EXPECTATION_CLASSES = {
    "expect_column_values_to_not_be_null": (gx_expectations.ExpectColumnValuesToNotBeNull),
    "expect_column_values_to_be_between": (gx_expectations.ExpectColumnValuesToBeBetween),
    "expect_column_values_to_be_in_set": gx_expectations.ExpectColumnValuesToBeInSet,
    "expect_table_row_count_to_be_between": (gx_expectations.ExpectTableRowCountToBeBetween),
    "expect_column_proportion_of_unique_values_to_be_between": (
        gx_expectations.ExpectColumnProportionOfUniqueValuesToBeBetween
    ),
}


def _load_suite_definition(suite_name: str) -> list[dict]:
    path = _SUITE_DIR / f"{suite_name}.json"
    suite = json.loads(path.read_text(encoding="utf-8"))
    return suite["expectations"]


def _build_suite(suite_name: str):
    """Materialise the JSON suite as a real GX ExpectationSuite."""
    context = gx.get_context(mode="ephemeral")
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    for rule in _load_suite_definition(suite_name):
        expectation_type = rule["expectation_type"]
        expectation_class = _EXPECTATION_CLASSES.get(expectation_type)
        if expectation_class is None:
            raise ValueError(f"unsupported expectation type: {expectation_type}")
        suite.add_expectation(expectation_class(**rule["kwargs"]))
    return context, suite


def validate_frame(
    df: pd.DataFrame, suite_name: str = "fraud_transactions_suite"
) -> tuple[bool, list[str]]:
    """Run the suite against *df*.

    Returns (passed, failures) where each failure is "expectation_type:column".
    """
    context, suite = _build_suite(suite_name)

    datasource = context.data_sources.add_pandas(name="fraud_transactions")
    asset = datasource.add_dataframe_asset(name="batch")
    batch_definition = asset.add_batch_definition_whole_dataframe(name="whole_frame")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    result = batch.validate(suite)
    failures = [
        f"{item.expectation_config.type}:{item.expectation_config.kwargs.get('column', '')}"
        for item in result.results
        if not item.success
    ]
    return result.success, failures


def _read_frame(parquet_path: str) -> pd.DataFrame:
    if parquet_path.startswith("s3://"):
        return read_parquet_keys([parquet_path])
    return pd.read_parquet(parquet_path)


def main(parquet_path: str) -> int:
    ok, failures = validate_frame(_read_frame(parquet_path))
    if ok:
        print(f"✓ Validation passed for {parquet_path}")
        return 0

    print(f"✗ Validation FAILED for {parquet_path}", file=sys.stderr)
    print("Failed expectations:", json.dumps(failures, indent=2), file=sys.stderr)
    return 1


if __name__ == "__main__":
    import typer

    typer.run(main)
