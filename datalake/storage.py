"""Single access layer for the S3-compatible data lake (SeaweedFS).

Every process that touches the lake — ingestion, training, drift, the API and the
operator scripts — goes through this module, so endpoint/credential handling and
the key layout exist in exactly one place.

Layout:
  raw/*.parquet         validated transactions written by ingestion.lake_consumer
  quarantine/*.parquet  batches that failed the fast pre-checks
  features/*.parquet    point-in-time velocity aggregates written by features.compute
  drift_reports/<date>/ Evidently reports uploaded by monitoring.run_drift
"""

from __future__ import annotations

import io
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow.parquet as pq

from config.settings import get_settings

SEED_PREFIX = "raw/seed_"


def s3_client():
    """Return a boto3 S3 client pointing at the configured SeaweedFS endpoint."""
    cfg = get_settings()
    return boto3.client(
        "s3",
        endpoint_url=cfg.seaweed_endpoint,
        aws_access_key_id=cfg.seaweed_access_key,
        aws_secret_access_key=cfg.seaweed_secret_key,
    )


def _objects(prefix: str) -> list[dict]:
    """List every object under *prefix*, newest last."""
    cfg = get_settings()
    paginator = s3_client().get_paginator("list_objects_v2")
    objects = [
        obj
        for page in paginator.paginate(Bucket=cfg.datalake_bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
    ]
    return sorted(objects, key=lambda o: o["LastModified"])


def list_parquet(prefix: str) -> list[str]:
    """Return the parquet keys under *prefix*, newest last."""
    return [o["Key"] for o in _objects(prefix) if o["Key"].endswith(".parquet")]


def parquet_columns(key: str) -> list[str]:
    """Column names at *key*, read from the parquet footer only (no full download)."""
    cfg = get_settings()
    body = (
        s3_client().get_object(Bucket=cfg.datalake_bucket, Key=_normalise_key(key))["Body"].read()
    )
    return list(pq.read_schema(io.BytesIO(body)).names)


def _normalise_key(key: str) -> str:
    """Accept both bare keys and full ``s3://bucket/key`` URIs."""
    cfg = get_settings()
    prefix = f"s3://{cfg.datalake_bucket}/"
    return key[len(prefix) :] if key.startswith(prefix) else key


def read_parquet_keys(keys: list[str]) -> pd.DataFrame:
    """Concatenate the parquet objects at *keys* into one frame."""
    cfg = get_settings()
    client = s3_client()
    frames = []
    for key in keys:
        body = client.get_object(Bucket=cfg.datalake_bucket, Key=_normalise_key(key))["Body"].read()
        frames.append(pd.read_parquet(io.BytesIO(body)))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def write_parquet(df: pd.DataFrame, key: str) -> str:
    """Write *df* as snappy parquet at *key* and return its ``s3://`` URI."""
    cfg = get_settings()
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy", engine="pyarrow")
    buffer.seek(0)
    s3_client().put_object(
        Bucket=cfg.datalake_bucket,
        Key=_normalise_key(key),
        Body=buffer.read(),
        ContentType="application/octet-stream",
    )
    return f"s3://{cfg.datalake_bucket}/{_normalise_key(key)}"


def write_bytes(data: bytes, key: str, content_type: str) -> str:
    """Write raw *data* at *key* and return its ``s3://`` URI."""
    cfg = get_settings()
    s3_client().put_object(
        Bucket=cfg.datalake_bucket,
        Key=_normalise_key(key),
        Body=data,
        ContentType=content_type,
    )
    return f"s3://{cfg.datalake_bucket}/{_normalise_key(key)}"


def lake_status() -> dict:
    """Summarise the lake: file counts, size, seed count and last raw write.

    Raises the underlying botocore/OSError when the lake is unreachable; callers
    decide how to report that (the API turns it into a 503).
    """
    raw = _objects("raw/")
    quarantine = _objects("quarantine/")
    raw_parquet = [o for o in raw if o["Key"].endswith(".parquet")]
    last_write = raw_parquet[-1]["LastModified"] if raw_parquet else None
    return {
        "lake_raw_files": len(raw_parquet),
        "lake_raw_size_mb": round(sum(o["Size"] for o in raw_parquet) / 1_048_576, 2),
        "seed_files": sum(1 for o in raw_parquet if o["Key"].startswith(SEED_PREFIX)),
        "quarantine_files": sum(1 for o in quarantine if o["Key"].endswith(".parquet")),
        "last_raw_write": (
            last_write.astimezone(timezone.utc).isoformat()
            if isinstance(last_write, datetime)
            else None
        ),
    }
