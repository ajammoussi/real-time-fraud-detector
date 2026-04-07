"""Feast feature definitions for the fraud-detection velocity features.

The offline source is the pair of parquet files written by features/compute.py:
point-in-time aggregates over a trailing 7-day window, keyed by entity and
window end. Feast's `ttl` supplies the trailing window at retrieval time.

`online=True` keeps the latest values in Redis for serving; historical
retrieval reads the parquet files directly, so CI needs no Redis.

The module is deliberately not named `features.py`: `feast apply` imports repo
modules by module name, and the images put `/app` on PYTHONPATH, so a local
`features.py` would be shadowed by the project's `features/` package — apply
would then no-op and write an empty registry.
"""

from __future__ import annotations

import os
from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, ValueType
from feast.types import Float32

_S3_ENDPOINT = os.environ.get("SEAWEED_ENDPOINT", "http://seaweedfs-s3:8333")

user_stats_source = FileSource(
    name="user_stats_source",
    path="s3://datalake/features/user_stats.parquet",
    timestamp_field="event_timestamp",
    s3_endpoint_override=_S3_ENDPOINT,
)

merchant_stats_source = FileSource(
    name="merchant_stats_source",
    path="s3://datalake/features/merchant_stats.parquet",
    timestamp_field="event_timestamp",
    s3_endpoint_override=_S3_ENDPOINT,
)

user = Entity(name="user_id", join_keys=["user_id"], value_type=ValueType.STRING)
merchant = Entity(name="merchant_id", join_keys=["merchant_id"], value_type=ValueType.STRING)

user_stats = FeatureView(
    name="user_stats",
    entities=[user],
    ttl=timedelta(days=7),
    online=True,
    source=user_stats_source,
    schema=[
        Field(name="user_txn_count_7d", dtype=Float32),
        Field(name="user_amount_sum_7d", dtype=Float32),
        Field(name="user_amount_avg_7d", dtype=Float32),
        Field(name="user_amount_max_7d", dtype=Float32),
    ],
)

merchant_stats = FeatureView(
    name="merchant_stats",
    entities=[merchant],
    ttl=timedelta(days=7),
    online=True,
    source=merchant_stats_source,
    schema=[
        Field(name="merchant_txn_count_7d", dtype=Float32),
        Field(name="merchant_amount_avg_7d", dtype=Float32),
    ],
)
