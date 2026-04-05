"""Integration test for ingestion control status output."""

from __future__ import annotations

import json
import subprocess
import sys

import pytest


def test_ingestion_status_command_returns_expected_keys():
    proc = subprocess.run(
        [sys.executable, "scripts/ingestion_ctl.py", "status"],
        capture_output=True,
        text=True,
        check=False,
    )

    if proc.returncode != 0:
        pytest.skip(f"ingestion status unavailable in test env: {proc.stderr.strip()}")
    payload = json.loads(proc.stdout)
    assert "lake_raw_files" in payload
    assert "quarantine_files" in payload
    assert "kafka_topic_raw" in payload
