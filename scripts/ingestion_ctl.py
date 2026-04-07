"""Ingestion health-check and producer control utility.

Commands:
  1. `status` — report what the data lake currently holds (JSON)
  2. `latest` — print the s3:// URI of the newest raw parquet (empty if none)
  3. `start`  — launch the Binance WebSocket producer as a background process
     (local dev helper; in Docker it runs as the `binance-producer` service)
  4. `seed`   — one-time synthetic bootstrap for an empty lake. This is the
     ONLY place fraud labels are created: the public Binance feed is unlabelled.

Usage:
    python scripts/ingestion_ctl.py status
    python scripts/ingestion_ctl.py latest
    python scripts/ingestion_ctl.py start [--symbols btcusdt,ethusdt]
    python scripts/ingestion_ctl.py seed --n-rows 5000
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import typer

from config.settings import get_settings
from datalake.storage import lake_status, list_parquet, write_parquet

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
    """Show what the data lake holds, as JSON."""
    try:
        print(json.dumps(lake_status(), indent=2))
    except Exception as exc:
        print(f"Error reaching the data lake: {exc}", file=sys.stderr)
        raise typer.Exit(code=1) from exc


@app.command()
def latest():
    """Print the s3:// URI of the newest raw parquet, or nothing if the lake is empty."""
    cfg = get_settings()
    try:
        keys = list_parquet("raw/")
    except Exception as exc:
        print(f"Error reaching the data lake: {exc}", file=sys.stderr)
        raise typer.Exit(code=1) from exc

    print(f"s3://{cfg.datalake_bucket}/{keys[-1]}" if keys else "")


@app.command()
def start(
    symbols: str = typer.Option("", help="Override BINANCE_SYMBOLS env var"),
    detach: bool = typer.Option(False, help="Run in background (nohup)"),
):
    """Start the Binance WebSocket producer (local dev helper)."""
    cmd = [sys.executable, "ingestion/producer.py"]
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
    """ONE-TIME bootstrap: write synthetic, labelled seed data to the lake.

    The live feed is unlabelled, so this is the only source of the `label`
    column the classifier trains on. Use it only when starting with zero
    historical data — never to top up a lake that already has rows.
    """
    rng = np.random.default_rng(rng_seed)
    n_fraud = max(1, int(n_rows * fraud_rate))
    rows = [_seed_row(rng, True) for _ in range(n_fraud)]
    rows += [_seed_row(rng, False) for _ in range(n_rows - n_fraud)]
    rng.shuffle(rows)
    df = pd.DataFrame(rows)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = write_parquet(df, f"raw/seed_txn_{ts}.parquet")
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
                "note": "Bootstrap only — live data arrives via ingestion/lake_consumer.py",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    app()
