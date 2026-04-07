"""Idempotent writes of scored transactions into the prediction audit table."""

from __future__ import annotations

import json

from psycopg2.pool import SimpleConnectionPool

from config.settings import get_settings

_pool: SimpleConnectionPool | None = None


def get_pool() -> SimpleConnectionPool:
    """Return the lazily created connection pool."""
    global _pool
    if _pool is None:
        _pool = SimpleConnectionPool(1, 5, get_settings().postgres_dsn)
    return _pool


def record_prediction(
    *,
    transaction_id: str,
    model_version: str,
    fraud_prob: float,
    decision: str,
    features: dict,
    shadow_prob: float | None,
    latency_ms: float,
) -> bool:
    """Insert one prediction, ignoring replays of an already scored transaction.

    Returns True when the row was inserted, False when it was a duplicate.
    Exceptions propagate: callers decide whether a database hiccup is fatal
    (the prediction consumer) or merely logged (the HTTP handler).
    """
    pool = get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO predictions
                   (transaction_id, model_version, fraud_prob, decision,
                    features_json, shadow_prob, latency_ms)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (transaction_id) DO NOTHING""",
                (
                    transaction_id,
                    model_version,
                    fraud_prob,
                    decision,
                    json.dumps(features),
                    shadow_prob,
                    latency_ms,
                ),
            )
            inserted = cur.rowcount > 0
        conn.commit()
        return inserted
    finally:
        pool.putconn(conn)
