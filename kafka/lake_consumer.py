"""Streaming-to-lake Kafka consumer.

Subscribes to `transactions_raw`, accumulates messages into micro-batches,
runs Great Expectations validation on each batch, converts passing batches
to Parquet, and writes them to SeaweedFS (s3://datalake/raw/).

This is the ONLY process that populates the raw/ prefix in the data lake.
Training and retraining workflows simply read whatever Parquet files have
accumulated there — no explicit "generate data" step required.

Flush triggers (whichever fires first):
  • lake_batch_size    messages consumed  (default: 1 000)
  • lake_batch_timeout seconds elapsed    (default: 300 s / 5 min)

On validation pass  → write to s3://datalake/raw/realtime_txn_<ISO_TS>.parquet
On validation fail  → write to s3://datalake/quarantine/<ISO_TS>.parquet + log
"""

from __future__ import annotations

import io
import json
import logging
import signal
import time
from datetime import datetime, timezone
from typing import Optional

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError
from kafka.errors import KafkaError

from config.settings import get_settings
from kafka import KafkaConsumer

log = logging.getLogger("lake-consumer")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

# Columns that must be dropped before Parquet write (provenance-only)
_PROVENANCE_COLS = {"_source", "_symbol", "_trade_id", "_price", "_quantity"}

# Required schema columns — GX will verify these but we also pre-check here
_REQUIRED_COLS = {
    "transaction_id",
    "timestamp",
    "user_id",
    "merchant_id",
    "merchant_cat",
    "amount_usd",
    "currency",
    "country",
    "device_type",
    "ip_hash",
    "card_last4",
    "is_international",
    "hour_of_day",
    "day_of_week",
    "label",
}

_VALID_DEVICE_TYPES = {
    "mobile",
    "desktop",
    "tablet",
    "pos_terminal",
    "exchange_maker",
    "exchange_taker",
}


# ─── S3 helpers ────────────────────────────────────────────────────────────


def _s3_client():
    cfg = get_settings()
    return boto3.client(
        "s3",
        endpoint_url=cfg.seaweed_endpoint,
        aws_access_key_id=cfg.seaweed_access_key,
        aws_secret_access_key=cfg.seaweed_secret_key,
    )


def _write_parquet_to_s3(df: pd.DataFrame, s3_key: str) -> str:
    """Serialise *df* as Snappy-compressed Parquet and PUT to SeaweedFS."""
    cfg = get_settings()
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy", engine="pyarrow")
    buf.seek(0)
    client = _s3_client()
    client.put_object(
        Bucket=cfg.datalake_bucket,
        Key=s3_key,
        Body=buf.read(),
        ContentType="application/octet-stream",
    )
    return f"s3://{cfg.datalake_bucket}/{s3_key}"


# ─── Great Expectations (lightweight inline) ───────────────────────────────


def _validate_batch(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """Run inline validation rules that mirror the GX expectation suite.

    Returns (passed: bool, failed_checks: list[str]).
    We run inline checks here instead of full GX context for speed;
    the authoritative GX suite in gx/ is still used in CI.
    """
    failed: list[str] = []

    # 1. Required columns present
    missing = _REQUIRED_COLS - set(df.columns)
    if missing:
        failed.append(f"missing_columns:{missing}")

    if failed:
        return False, failed

    # 2. No nulls in critical columns
    for col in ["transaction_id", "amount_usd", "label", "timestamp"]:
        if df[col].isnull().any():
            failed.append(f"nulls_in:{col}")

    # 3. amount_usd in [0.01, 50 000]
    bad_amount = df[(df["amount_usd"] < 0.01) | (df["amount_usd"] > 50_000)]
    if len(bad_amount):
        failed.append(f"amount_out_of_range:{len(bad_amount)}_rows")

    # 4. transaction_id uniqueness ≥ 99 %
    uniq_ratio = df["transaction_id"].nunique() / len(df)
    if uniq_ratio < 0.99:
        failed.append(f"low_txn_id_uniqueness:{uniq_ratio:.3f}")

    # 5. label ∈ {0, 1}
    bad_labels = df[~df["label"].isin([0, 1])]
    if len(bad_labels):
        failed.append(f"invalid_labels:{len(bad_labels)}_rows")

    # 6. hour_of_day ∈ [0, 23]
    if not df["hour_of_day"].between(0, 23).all():
        failed.append("hour_of_day_out_of_range")

    # 7. device_type in allowed set
    bad_device_types = df[~df["device_type"].isin(_VALID_DEVICE_TYPES)]
    if len(bad_device_types):
        failed.append(f"invalid_device_type:{len(bad_device_types)}_rows")

    return len(failed) == 0, failed


# ─── Batch flush ───────────────────────────────────────────────────────────


def _flush_batch(records: list[dict]) -> Optional[str]:
    """Validate and persist *records* to the data lake.

    Returns the S3 path on success, None on failure.
    """
    if not records:
        return None

    df = pd.DataFrame(records)

    # Drop provenance-only columns before writing
    drop_cols = [c for c in _PROVENANCE_COLS if c in df.columns]
    df = df.drop(columns=drop_cols)

    # Coerce types
    df["amount_usd"] = df["amount_usd"].astype(float)
    df["hour_of_day"] = df["hour_of_day"].astype(int)
    df["day_of_week"] = df["day_of_week"].astype(int)
    df["is_international"] = df["is_international"].astype(bool)
    df["label"] = df["label"].astype(int)

    passed, failures = _validate_batch(df)
    iso_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if passed:
        s3_key = f"raw/realtime_txn_{iso_ts}.parquet"
        log.info("✓ Validation passed (%d rows) → %s", len(df), s3_key)
    else:
        s3_key = f"quarantine/realtime_txn_{iso_ts}.parquet"
        log.warning(
            "✗ Validation FAILED (%d rows) → %s | checks: %s",
            len(df),
            s3_key,
            failures,
        )

    try:
        path = _write_parquet_to_s3(df, s3_key)
        log.info("Wrote %d rows → %s", len(df), path)
        return path if passed else None
    except (BotoCoreError, ClientError) as exc:
        log.error("S3 write failed: %s", exc)
        return None


# ─── Main consumer loop ────────────────────────────────────────────────────


def run() -> None:
    cfg = get_settings()

    consumer = KafkaConsumer(
        cfg.kafka_topic_transactions_raw,
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        group_id=cfg.kafka_consumer_group_lake,
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # manual commit after flush
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        fetch_max_wait_ms=500,
        max_poll_records=500,
    )

    log.info(
        "Lake consumer started | topic=%s | batch_size=%d | timeout=%ds",
        cfg.kafka_topic_transactions_raw,
        cfg.lake_batch_size,
        cfg.lake_batch_timeout_secs,
    )

    buffer: list[dict] = []
    batch_start = time.monotonic()
    running = True
    batches_written = 0
    total_msgs = 0

    def _shutdown(sig, _frame):
        nonlocal running
        log.info("Received signal %s — draining buffer and shutting down …", sig)
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        while running:
            # Poll with a short timeout so we can check the time-based flush condition
            msg_pack = consumer.poll(timeout_ms=2_000)

            for _tp, messages in msg_pack.items():
                for msg in messages:
                    if msg.value:
                        buffer.append(msg.value)
                        total_msgs += 1

            elapsed = time.monotonic() - batch_start
            size_trigger = len(buffer) >= cfg.lake_batch_size
            time_trigger = elapsed >= cfg.lake_batch_timeout_secs and buffer

            if size_trigger or time_trigger:
                trigger = "size" if size_trigger else "timeout"
                log.info(
                    "Flushing batch (%s trigger) | %d records in buffer | "
                    "elapsed=%.1fs",
                    trigger,
                    len(buffer),
                    elapsed,
                )
                _flush_batch(buffer)
                consumer.commit()
                buffer.clear()
                batch_start = time.monotonic()
                batches_written += 1

    except KafkaError as exc:
        log.error("Kafka error: %s", exc)
    finally:
        # Drain remaining buffer on shutdown
        if buffer:
            log.info("Draining %d remaining records before exit …", len(buffer))
            _flush_batch(buffer)
            consumer.commit()
        consumer.close()
        log.info(
            "Lake consumer stopped | total_msgs=%d | batches_written=%d",
            total_msgs,
            batches_written,
        )


if __name__ == "__main__":
    run()
