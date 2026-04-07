"""`latest --requires label` must skip unlabelled live batches."""

from __future__ import annotations

from typer.testing import CliRunner

import scripts.ingestion_ctl as ctl

runner = CliRunner()

BATCHES = [
    "raw/seed_txn_20260407T090000Z.parquet",
    "raw/realtime_txn_20260407T120000Z.parquet",  # newest, no labels
]


def _columns(key: str) -> list[str]:
    return (
        ["transaction_id", "amount_usd", "label"]
        if "seed_" in key
        else ["transaction_id", "amount_usd"]
    )


def test_latest_requires_label_picks_the_newest_labelled_batch(monkeypatch):
    monkeypatch.setattr(ctl, "list_parquet", lambda prefix: list(BATCHES))
    monkeypatch.setattr(ctl, "parquet_columns", _columns)

    result = runner.invoke(ctl.app, ["latest", "--requires", "label"])

    assert result.exit_code == 0, result.stdout
    assert result.stdout.strip().endswith(BATCHES[0])


def test_latest_without_requires_returns_the_newest_batch(monkeypatch):
    monkeypatch.setattr(ctl, "list_parquet", lambda prefix: list(BATCHES))
    monkeypatch.setattr(ctl, "parquet_columns", _columns)

    result = runner.invoke(ctl.app, ["latest"])

    assert result.exit_code == 0, result.stdout
    assert result.stdout.strip().endswith(BATCHES[1])


def test_latest_requires_label_is_empty_when_nothing_is_labelled(monkeypatch):
    monkeypatch.setattr(ctl, "list_parquet", lambda prefix: [BATCHES[1]])
    monkeypatch.setattr(ctl, "parquet_columns", _columns)

    result = runner.invoke(ctl.app, ["latest", "--requires", "label"])

    assert result.exit_code == 0, result.stdout
    assert result.stdout.strip() == ""
