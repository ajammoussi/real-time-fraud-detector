"""Validate a Parquet file against gx/expectations/fraud_transactions_suite.json."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

from config.settings import get_settings


def _load_suite() -> list[dict]:
    suite_path = (
        Path(__file__).parent / "expectations" / "fraud_transactions_suite.json"
    )
    suite = json.loads(suite_path.read_text(encoding="utf-8"))
    return suite.get("expectations", [])


def _load_parquet(parquet_path: str) -> pd.DataFrame:
    if parquet_path.startswith("s3://"):
        import s3fs

        cfg = get_settings()
        fs = s3fs.S3FileSystem(
            key=cfg.seaweed_access_key,
            secret=cfg.seaweed_secret_key,
            endpoint_url=cfg.seaweed_endpoint,
        )
        return pd.read_parquet(parquet_path, filesystem=fs)
    return pd.read_parquet(parquet_path)


def validate_parquet(parquet_path: str) -> bool:
    df = _load_parquet(parquet_path)
    expectations = _load_suite()

    failures: list[str] = []

    for rule in expectations:
        etype = rule.get("expectation_type")
        kwargs = rule.get("kwargs", {})

        if etype == "expect_column_values_to_not_be_null":
            col = kwargs["column"]
            if col not in df.columns or df[col].isnull().any():
                failures.append(f"{etype}:{col}")

        elif etype == "expect_column_values_to_be_between":
            col = kwargs["column"]
            min_v = kwargs.get("min_value")
            max_v = kwargs.get("max_value")
            if col not in df.columns:
                failures.append(f"{etype}:{col}:missing")
            else:
                mask = pd.Series(True, index=df.index)
                if min_v is not None:
                    mask &= df[col] >= min_v
                if max_v is not None:
                    mask &= df[col] <= max_v
                if not mask.all():
                    failures.append(f"{etype}:{col}")

        elif etype == "expect_column_values_to_be_in_set":
            col = kwargs["column"]
            allowed = set(kwargs.get("value_set", []))
            if col not in df.columns or not df[col].isin(allowed).all():
                failures.append(f"{etype}:{col}")

        elif etype == "expect_table_row_count_to_be_between":
            min_v = kwargs.get("min_value", 0)
            max_v = kwargs.get("max_value", float("inf"))
            n_rows = len(df)
            if not (min_v <= n_rows <= max_v):
                failures.append(f"{etype}:rows={n_rows}")

        elif etype == "expect_column_proportion_of_unique_values_to_be_between":
            col = kwargs["column"]
            min_v = kwargs.get("min_value", 0.0)
            max_v = kwargs.get("max_value", 1.0)
            if col not in df.columns or len(df) == 0:
                failures.append(f"{etype}:{col}")
            else:
                ratio = df[col].nunique(dropna=True) / len(df)
                if not (min_v <= ratio <= max_v):
                    failures.append(f"{etype}:{col}:ratio={ratio:.4f}")

    if not failures:
        print(f"✓ Validation passed for {parquet_path}")
        return True

    print(f"✗ Validation FAILED for {parquet_path}", file=sys.stderr)
    print("Failed expectations:", json.dumps(failures, indent=2), file=sys.stderr)
    return False


if __name__ == "__main__":
    import typer

    def _main(parquet_path: str) -> None:
        ok = validate_parquet(parquet_path)
        raise typer.Exit(code=0 if ok else 1)

    typer.run(_main)
