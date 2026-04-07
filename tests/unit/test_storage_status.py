"""Lake status must count what the lake actually holds."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import datalake.storage as storage


class _Paginator:
    def __init__(self, objects):
        self.objects = objects

    def paginate(self, Bucket, Prefix):
        return [{"Contents": [o for o in self.objects if o["Key"].startswith(Prefix)]}]


class _FakeS3:
    def __init__(self, objects):
        self.objects = objects

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _Paginator(self.objects)


def test_lake_status_counts_seed_files_separately(monkeypatch):
    now = datetime.now(timezone.utc)
    objects = [
        {
            "Key": "raw/realtime_txn_a.parquet",
            "Size": 1_048_576,
            "LastModified": now - timedelta(hours=2),
        },
        {"Key": "raw/seed_txn_b.parquet", "Size": 524_288, "LastModified": now},
        # non-parquet objects must not be counted
        {"Key": "raw/_SUCCESS", "Size": 0, "LastModified": now},
        {"Key": "quarantine/realtime_txn_c.parquet", "Size": 100, "LastModified": now},
    ]
    monkeypatch.setattr(storage, "s3_client", lambda: _FakeS3(objects))

    status = storage.lake_status()

    assert status["lake_raw_files"] == 2
    assert status["lake_raw_size_mb"] == 1.5
    assert status["seed_files"] == 1
    assert status["quarantine_files"] == 1
    assert status["last_raw_write"] == now.isoformat()


def test_empty_lake_reports_nothing_written(monkeypatch):
    monkeypatch.setattr(storage, "s3_client", lambda: _FakeS3([]))
    status = storage.lake_status()
    assert status["lake_raw_files"] == 0
    assert status["seed_files"] == 0
    assert status["last_raw_write"] is None
