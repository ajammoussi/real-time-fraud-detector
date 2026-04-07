"""Prediction Kafka consumer — scores raw trades and persists the audit trail.

Two independent consumer groups read `transactions_raw`:

  1. ingestion/lake_consumer.py  (group: lake-writers)
       Batches records → pre-checks → Parquet in SeaweedFS. Builds the training
       data lake; no model involved.

  2. ingestion/predict_consumer.py  (group: prediction-workers)  ← THIS FILE
       Scores every record through the same api.inference.score path the HTTP
       API uses, and writes the result to the `predictions` table for
       monitoring and drift detection.

Both groups advance their offsets independently, so every message is processed
by both pipelines. Offsets are committed per batch, after the rows are written;
the unique index on transaction_id makes a replayed batch idempotent.
"""

from __future__ import annotations

import json
import logging
import signal
import time

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server

from api.inference import score
from api.model_loader import ModelUnavailable, get_model_info, load_model
from config.settings import get_settings
from db.writer import record_prediction

log = logging.getLogger("predict-consumer")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

# Each consumer process owns its own registry: the metric names must stay
# collision-free when both modules are imported into one process (tests, CI).
REGISTRY = CollectorRegistry()

consumer_messages_processed_total = Counter(
    "consumer_messages_processed_total",
    "Messages scored and persisted",
    registry=REGISTRY,
)
consumer_errors_total = Counter(
    "consumer_errors_total",
    "Messages that failed to score or persist",
    registry=REGISTRY,
)
consumer_duplicates_total = Counter(
    "consumer_duplicates_total",
    "Messages skipped by the transaction_id unique index",
    registry=REGISTRY,
)
kafka_consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Messages this consumer is behind the topic head",
    registry=REGISTRY,
)


def _wait_for_model(retry_secs: int = 10):
    """Block until a servable model exists instead of crash-looping the container."""
    while True:
        try:
            return load_model()
        except ModelUnavailable as exc:
            log.warning("model not ready yet (%s); retrying in %ds", exc, retry_secs)
            time.sleep(retry_secs)


def _refresh_lag(consumer: KafkaConsumer) -> None:
    """Publish end_offset - committed_offset for the current assignment."""
    assignment = consumer.assignment()
    if not assignment:
        return
    end_offsets = consumer.end_offsets(assignment)
    # KafkaConsumer.committed() takes one TopicPartition, not an iterable.
    committed = {tp: consumer.committed(tp) for tp in assignment}
    lag = sum(max(end_offsets[tp] - (committed.get(tp) or 0), 0) for tp in assignment)
    kafka_consumer_lag.set(lag)


def _process(tx: dict, model, info: dict) -> None:
    """Score one transaction and persist it."""
    started = time.perf_counter()
    result = score(tx, model=model, info=info)
    latency_ms = (time.perf_counter() - started) * 1000

    inserted = record_prediction(
        transaction_id=tx["transaction_id"],
        model_version=result["model_version"],
        fraud_prob=result["fraud_probability"],
        decision=result["decision"],
        features=result["model_features"],
        shadow_prob=None,
        latency_ms=latency_ms,
    )
    if inserted:
        consumer_messages_processed_total.inc()
    else:
        consumer_duplicates_total.inc()


def run() -> None:
    cfg = get_settings()
    start_http_server(cfg.consumer_metrics_port, registry=REGISTRY)
    log.info("Metrics endpoint listening on :%d", cfg.consumer_metrics_port)

    consumer = KafkaConsumer(
        cfg.kafka_topic_transactions_raw,  # same topic as the lake consumer
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        group_id=cfg.kafka_consumer_group,  # different group → independent offset
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # commit only after the batch is persisted
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        max_poll_records=100,
        fetch_max_wait_ms=500,
    )

    running = True

    def _shutdown(sig, _frame):
        nonlocal running
        log.info("Received signal %s — finishing the current batch …", sig)
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    _wait_for_model()
    log.info("Prediction consumer running on %s", cfg.kafka_topic_transactions_raw)

    try:
        while running:
            msg_pack = consumer.poll(timeout_ms=1_000)
            _refresh_lag(consumer)

            batch = [msg.value for messages in msg_pack.values() for msg in messages if msg.value]
            if not batch:
                continue

            try:
                model = load_model()
                info = get_model_info()
            except ModelUnavailable as exc:
                # Do not commit: retry this batch once a model is servable again.
                log.error("model unavailable, holding the batch: %s", exc)
                consumer_errors_total.inc(len(batch))
                _wait_for_model()
                continue

            for tx in batch:
                try:
                    _process(tx, model, info)
                except Exception as exc:
                    consumer_errors_total.inc()
                    log.error("failed to score %s: %s", tx.get("transaction_id"), exc)

            consumer.commit()

    except KafkaError as exc:
        log.error("Kafka error: %s", exc)
    finally:
        consumer.close()
        log.info("Prediction consumer stopped")


if __name__ == "__main__":
    run()
