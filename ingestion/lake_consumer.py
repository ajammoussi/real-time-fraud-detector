"""Streaming-to-lake Kafka consumer.

Subscribes to `transactions_raw`, accumulates messages into micro-batches,
runs the fast schema pre-checks on each batch, converts passing batches to
Parquet, and writes them to SeaweedFS (s3://datalake/raw/).

This is the ONLY process that populates the raw/ prefix in the data lake.
Training and retraining read whatever Parquet files have accumulated there.

Flush triggers (whichever fires first):
  • lake_batch_size    messages consumed  (default: 1 000)
  • lake_batch_timeout seconds elapsed    (default: 300 s / 5 min)

On pre-check pass → s3://datalake/raw/realtime_txn_<ISO_TS>.parquet
On pre-check fail → s3://datalake/quarantine/<ISO_TS>.parquet (+ warning log)

The pre-checks here are deliberately cheap: they are the fast gate that keeps
obviously-broken rows out of the lake. The authoritative contract — the Great
Expectations suite over the assembled, labelled training frame — runs in
training/build_dataset.py, where a failure blocks training instead of merely
diverting a batch.

Offsets are committed only after the batch reached the lake. A failed S3 write
leaves the offset uncommitted so the messages are redelivered on restart.
"""

from __future__ import annotations

import json
import logging
import signal
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server

from config.settings import get_settings
from datalake.storage import write_parquet

log = logging.getLogger("lake-consumer")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

METRICS_PORT = 8002

# Each consumer process owns its own registry: the metric names must stay
# collision-free when both modules are imported into one process (tests, CI).
REGISTRY = CollectorRegistry()

lake_rows_written_total = Counter(
    "lake_rows_written_total",
    "Rows successfully written to the raw lake prefix",
    registry=REGISTRY,
)
lake_batches_written_total = Counter(
    "lake_batches_written_total",
    "Batches successfully written to the raw lake prefix",
    registry=REGISTRY,
)
kafka_consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Messages this consumer is behind the topic head",
    registry=REGISTRY,
)

# Columns that must be dropped before Parquet write (provenance-only)
_PROVENANCE_COLS = {"_source", "_symbol", "_trade_id", "_price", "_quantity"}

# Required schema columns — the lake record contract (labels are NOT here:
# a public trade feed carries none; only the seed path adds them)
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
}

_VALID_DEVICE_TYPES = {
    "mobile",
    "desktop",
    "tablet",
    "pos_terminal",
    "exchange_maker",
    "exchange_taker",
}


def _validate_batch(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """Run the fast pre-checks that decide lake vs quarantine.

    Returns (passed: bool, failed_checks: list[str]).
    """
    failed: list[str] = []

    # 1. Required columns present
    missing = _REQUIRED_COLS - set(df.columns)
    if missing:
        return False, [f"missing_columns:{missing}"]

    # 2. No nulls in critical columns
    for col in ["transaction_id", "amount_usd", "timestamp"]:
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

    # 5. hour_of_day ∈ [0, 23]
    if not df["hour_of_day"].between(0, 23).all():
        failed.append("hour_of_day_out_of_range")

    # 6. device_type in allowed set
    bad_device_types = df[~df["device_type"].isin(_VALID_DEVICE_TYPES)]
    if len(bad_device_types):
        failed.append(f"invalid_device_type:{len(bad_device_types)}_rows")

    return len(failed) == 0, failed


def _flush_batch(records: list[dict]) -> tuple[str, str]:
    """Validate and persist *records* to the data lake.

    Returns ("written" | "quarantined" | "failed", path_or_reason).
    """
    if not records:
        return "failed", "empty batch"

    df = pd.DataFrame(records)
    df = df.drop(columns=[c for c in _PROVENANCE_COLS if c in df.columns])

    # Coerce types
    df["amount_usd"] = df["amount_usd"].astype(float)
    df["hour_of_day"] = df["hour_of_day"].astype(int)
    df["day_of_week"] = df["day_of_week"].astype(int)
    df["is_international"] = df["is_international"].astype(bool)

    passed, failures = _validate_batch(df)
    iso_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if passed:
        s3_key = f"raw/realtime_txn_{iso_ts}.parquet"
        log.info("✓ Pre-checks passed (%d rows) → %s", len(df), s3_key)
    else:
        s3_key = f"quarantine/realtime_txn_{iso_ts}.parquet"
        log.warning(
            "✗ Pre-checks FAILED (%d rows) → %s | checks: %s",
            len(df),
            s3_key,
            failures,
        )

    try:
        path = write_parquet(df, s3_key)
    except Exception as exc:
        log.error("S3 write failed (%d rows): %s", len(df), exc)
        return "failed", str(exc)

    log.info("Wrote %d rows → %s", len(df), path)
    if not passed:
        return "quarantined", path

    lake_rows_written_total.inc(len(df))
    lake_batches_written_total.inc()
    return "written", path


def _refresh_lag(consumer: KafkaConsumer) -> None:
    """Publish end_offset - committed_offset for the current assignment."""
    assignment = consumer.assignment()
    if not assignment:
        return
    end_offsets = consumer.end_offsets(assignment)
    committed = consumer.committed(assignment)
    lag = sum(max(end_offsets[tp] - (committed.get(tp) or 0), 0) for tp in assignment)
    kafka_consumer_lag.set(lag)


def run() -> None:
    cfg = get_settings()

    start_http_server(METRICS_PORT, registry=REGISTRY)
    log.info("Metrics endpoint listening on :%d", METRICS_PORT)

    consumer = KafkaConsumer(
        cfg.kafka_topic_transactions_raw,
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        group_id=cfg.kafka_consumer_group_lake,
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # manual commit after the batch is durable
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
            _refresh_lag(consumer)

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
                    "Flushing batch (%s trigger) | %d records in buffer | elapsed=%.1fs",
                    trigger,
                    len(buffer),
                    elapsed,
                )
                status, detail = _flush_batch(buffer)
                buffer.clear()
                batch_start = time.monotonic()
                if status == "failed":
                    # Leave the offset uncommitted: the batch is redelivered
                    # after a restart instead of being silently lost.
                    log.error("Batch lost to the lake, offset not committed: %s", detail)
                else:
                    consumer.commit()
                    batches_written += 1

    except KafkaError as exc:
        log.error("Kafka error: %s", exc)
    finally:
        # Drain remaining buffer on shutdown
        if buffer:
            log.info("Draining %d remaining records before exit …", len(buffer))
            status, detail = _flush_batch(buffer)
            if status == "failed":
                log.error("Final batch lost to the lake: %s", detail)
            else:
                consumer.commit()
                batches_written += 1
        consumer.close()
        log.info(
            "Lake consumer stopped | total_msgs=%d | batches_written=%d",
            total_msgs,
            batches_written,
        )


if __name__ == "__main__":
    run()
