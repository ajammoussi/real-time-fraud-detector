"""Prediction Kafka consumer — pulls raw trades, scores them, stores results.

Architecture note
-----------------
There are now TWO independent consumer groups reading `transactions_raw`:

  1. kafka/lake_consumer.py  (group: lake-writers)
       Batches records → validates with GX → writes Parquet to SeaweedFS.
       Builds the training data lake. No model involvement.

  2. kafka/consumer.py       (group: prediction-workers)  ← THIS FILE
       Reads the same topic, scores every record with the live model,
       and writes results to the `predictions` PostgreSQL table for
       monitoring, drift detection and the shadow/A-B comparison.

Both groups advance their offsets independently, so every message is
processed by both pipelines without duplication.
"""

from __future__ import annotations

import json
import signal
import sys
import time

import pandas as pd
import psycopg2

from api.model_loader import get_model_info, load_model
from config.settings import get_settings
from kafka import KafkaConsumer
from training.feature_engineering import engineer_features, get_model_input_frame

cfg = get_settings()
running = True


def _get_pg_conn():
    return psycopg2.connect(cfg.postgres_dsn)


def _insert_prediction(conn, data: dict):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO predictions
               (transaction_id, model_version, fraud_prob, decision,
                features_json, latency_ms)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (
                data["transaction_id"],
                data["model_version"],
                data["fraud_prob"],
                data["decision"],
                json.dumps(data["features"]),
                data["latency_ms"],
            ),
        )
    conn.commit()


def _wait_for_model(retry_secs: int = 10):
    """Block until a model is available in MLflow instead of crashing the container."""
    while True:
        try:
            model = load_model()
            info = get_model_info()
            return model, info
        except Exception as exc:
            print(
                f"[consumer] model not ready yet ({exc}); retrying in {retry_secs}s",
                file=sys.stderr,
            )
            time.sleep(retry_secs)


def run():
    consumer = KafkaConsumer(
        cfg.kafka_topic_transactions_raw,  # same topic as lake_consumer
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        group_id=cfg.kafka_consumer_group,  # different group → independent offset
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=True,
    )
    conn = _get_pg_conn()
    model, info = _wait_for_model()
    print(
        "Prediction consumer running — reading from", cfg.kafka_topic_transactions_raw
    )

    def _shutdown(sig, frame):
        global running
        running = False
        consumer.close()
        conn.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    for message in consumer:
        if not running:
            break
        tx = message.value
        t0 = time.perf_counter()
        try:
            row = pd.DataFrame([tx])
            row = engineer_features(row)
            features = get_model_input_frame(row).values[0]
            prob = float(model.predict_proba(features.reshape(1, -1))[0, 1])
            decision = "REJECT" if prob >= 0.5 else "APPROVE"
            latency_ms = (time.perf_counter() - t0) * 1000
            _insert_prediction(
                conn,
                {
                    "transaction_id": tx.get("transaction_id"),
                    "model_version": str(info.get("version", "?")),
                    "fraud_prob": prob,
                    "decision": decision,
                    "features": tx,
                    "latency_ms": latency_ms,
                },
            )
        except Exception as exc:
            print(f"[consumer] error: {exc}", file=sys.stderr)


if __name__ == "__main__":
    run()
