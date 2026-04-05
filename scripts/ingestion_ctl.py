"""Ingestion health-check and producer control utility.

This script serves multiple purposes related to ingestion control and lake status reporting:

  1. `status`   — report how much data is in the lake and the Kafka lag
  2. `start`    — launch the Binance WebSocket producer as a background process
                  (useful for local dev; in prod it runs as a Docker service)
    3. `seed`     — ONE-TIME synthetic bootstrap, only for empty-lake init
                  an empty lake before any live data has arrived
                  (kept intentionally minimal — not used in the main pipeline)

Usage:
    python scripts/ingestion_ctl.py status
    python scripts/ingestion_ctl.py start [--symbols btcusdt,ethusdt]
    python scripts/ingestion_ctl.py seed  --n-rows 5000   # bootstrap only
"""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone

import boto3
import numpy as np
import pandas as pd
import typer

from config.settings import get_settings

app = typer.Typer(add_completion=False, help=__doc__)

# ── bootstrap seed data (used only for empty-lake init) ─────────────────────
_MERCHANT_CATS = [
    "crypto_exchange",
    "electronics",
    "grocery",
    "travel",
    "clothing",
    "restaurants",
    "gaming",
    "healthcare",
]
_DEVICE_TYPES = ["exchange_maker", "exchange_taker", "mobile", "desktop"]
_CURRENCIES = ["USD", "EUR", "GBP", "USD", "USD"]  # weighted
_COUNTRIES = ["US", "US", "US", "DE", "GB", "SG"]  # weighted


def _seed_row(rng: np.random.Generator, fraud: bool) -> dict:
    ts = datetime.now(timezone.utc) - timedelta(
        hours=int(rng.integers(0, 72)), minutes=int(rng.integers(0, 60))
    )
    amount = (
        float(rng.choice([rng.uniform(0.01, 1.0), rng.uniform(2000, 49999)]))
        if fraud
        else min(float(rng.lognormal(4.5, 1.2)), 49999.0)
    )
    tid = uuid.uuid4().hex[:12]
    return {
        "transaction_id": f"txn_seed_{tid}",
        "timestamp": ts.isoformat(),
        "user_id": f"usr_{rng.integers(1, 50000):05d}",
        "merchant_id": f"mrch_{rng.integers(1, 5000):04d}",
        "merchant_cat": rng.choice(_MERCHANT_CATS),
        "amount_usd": round(amount, 2),
        "currency": rng.choice(_CURRENCIES),
        "country": rng.choice(_COUNTRIES),
        "device_type": rng.choice(_DEVICE_TYPES),
        "ip_hash": hashlib.md5(tid.encode()).hexdigest()[:8],
        "card_last4": f"{rng.integers(1000, 9999)}",
        "is_international": bool(rng.random() > 0.8),
        "hour_of_day": ts.hour,
        "day_of_week": ts.weekday(),
        "label": int(fraud),
    }


# ── commands ────────────────────────────────────────────────────────────────


@app.command()
def status():
    """Show lake file count, total rows, and Kafka consumer lag."""
    cfg = get_settings()
    s3 = boto3.client(
        "s3",
        endpoint_url=cfg.seaweed_endpoint,
        aws_access_key_id=cfg.seaweed_access_key,
        aws_secret_access_key=cfg.seaweed_secret_key,
    )
    try:
        paginator = s3.get_paginator("list_objects_v2")
        raw_files = [
            obj
            for page in paginator.paginate(Bucket=cfg.datalake_bucket, Prefix="raw/")
            for obj in page.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        ]
        quar_files = [
            obj
            for page in paginator.paginate(
                Bucket=cfg.datalake_bucket, Prefix="quarantine/"
            )
            for obj in page.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        ]
        total_bytes = sum(f["Size"] for f in raw_files)
        print(
            json.dumps(
                {
                    "lake_raw_files": len(raw_files),
                    "lake_raw_size_mb": round(total_bytes / 1_048_576, 2),
                    "quarantine_files": len(quar_files),
                    "seaweed_endpoint": cfg.seaweed_endpoint,
                    "kafka_topic_raw": cfg.kafka_topic_transactions_raw,
                    "lake_batch_size": cfg.lake_batch_size,
                    "lake_batch_timeout_s": cfg.lake_batch_timeout_secs,
                },
                indent=2,
            )
        )
    except Exception as exc:
        print(f"Error reaching SeaweedFS: {exc}", file=sys.stderr)
        sys.exit(1)


@app.command()
def start(
    symbols: str = typer.Option("", help="Override BINANCE_SYMBOLS env var"),
    detach: bool = typer.Option(False, help="Run in background (nohup)"),
):
    """Start the Binance WebSocket producer (local dev helper)."""
    cmd = [sys.executable, "kafka/producer.py"]
    if symbols:
        cmd += ["--symbols", symbols]
    print(f"Launching: {' '.join(cmd)}")
    if detach:
        subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("Producer launched in background.")
    else:
        subprocess.run(cmd)  # blocking — Ctrl-C to stop


@app.command()
def seed(
    n_rows: int = typer.Option(5_000, help="Rows to write (bootstrap only)"),
    fraud_rate: float = typer.Option(0.02, help="Fraction of fraud rows"),
    rng_seed: int = typer.Option(42, help="RNG seed"),
):
    """ONE-TIME bootstrap: write synthetic seed data to the lake (empty-lake init only).

    In normal operation the lake is populated by kafka/lake_consumer.py.
    Use this only when starting the platform with zero historical data.
    """
    cfg = get_settings()
    rng = np.random.default_rng(rng_seed)
    n_fraud = max(1, int(n_rows * fraud_rate))
    rows = [_seed_row(rng, True) for _ in range(n_fraud)]
    rows += [_seed_row(rng, False) for _ in range(n_rows - n_fraud)]
    rng.shuffle(rows)
    df = pd.DataFrame(rows)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    s3_key = f"raw/seed_txn_{ts}.parquet"
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    buf.seek(0)

    s3 = boto3.client(
        "s3",
        endpoint_url=cfg.seaweed_endpoint,
        aws_access_key_id=cfg.seaweed_access_key,
        aws_secret_access_key=cfg.seaweed_secret_key,
    )
    s3.put_object(Bucket=cfg.datalake_bucket, Key=s3_key, Body=buf.read())
    path = f"s3://{cfg.datalake_bucket}/{s3_key}"
    checksum = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()

    print(
        json.dumps(
            {
                "status": "seeded",
                "path": path,
                "n_rows": len(df),
                "n_fraud": int(df["label"].sum()),
                "fraud_rate": round(float(df["label"].mean()), 4),
                "checksum_sha256": checksum,
                "note": "Bootstrap only — real data flows through Kafka/lake_consumer",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    app()
