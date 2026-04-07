"""Daily drift report over the model inputs that were actually served.

Reference window: predictions from 14 to 1 day ago. Current window: the last
day. Both windows are read from the `predictions` audit table, whose
`features_json` column holds the exact model input vector — so drift is
measured on what the model saw, not on the raw transaction.

Rows written before the audit contract changed hold raw transaction JSON; the
frame is built from the FEATURE_COLS subset of whatever keys are present, and
Evidently raises if a declared column is missing rather than silently drifting
on a smaller feature set.

Publishes `evidently_drift_share` to Prometheus (via Pushgateway) — the alert
rule `FraudDriftHigh` fires on that gauge.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import psycopg2
from evidently import DataDefinition, Dataset, Report
from evidently.presets import DataDriftPreset
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from config.settings import get_settings
from datalake.storage import write_bytes
from features.schema import VELOCITY_FEATURES
from training.feature_engineering import FEATURE_COLS

log = logging.getLogger("drift")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

MIN_WINDOW_ROWS = 200
REFERENCE_DAYS = 14
CURRENT_DAYS = 1

NUMERICAL_COLUMNS = [
    "amount_usd",
    "amount_log",
    "is_high_amount",
    "is_odd_hour",
    "is_weekend",
    "is_international",
    "hour_of_day",
    "day_of_week",
    "currency",
    *VELOCITY_FEATURES,
]
CATEGORICAL_COLUMNS = ["merchant_cat", "device_type"]

DATA_DEFINITION = DataDefinition(
    numerical_columns=NUMERICAL_COLUMNS, categorical_columns=CATEGORICAL_COLUMNS
)

REGISTRY = CollectorRegistry()
DRIFT_SHARE = Gauge(
    "evidently_drift_share",
    "Share of model-input columns Evidently flagged as drifted",
    registry=REGISTRY,
)


def _load_features(conn, start: datetime, end: datetime) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT features_json FROM predictions
               WHERE created_at >= %s AND created_at < %s""",
            (start, end),
        )
        return [row[0] for row in cur.fetchall()]


def _as_frame(rows: list[dict]) -> pd.DataFrame:
    """Keep only the columns that are model inputs."""
    frame = pd.DataFrame(rows)
    return frame[[column for column in FEATURE_COLS if column in frame.columns]]


def extract_drift_share(snapshot) -> float:
    """Pull DriftedColumnsCount.share out of an Evidently snapshot."""
    for metric in snapshot.dict().get("metrics", []):
        config = metric.get("config") or {}
        if str(config.get("type", "")).endswith("DriftedColumnsCount"):
            return float(metric["value"]["share"])
    raise ValueError("drift report contains no DriftedColumnsCount metric")


def run() -> float:
    cfg = get_settings()
    now = datetime.now(timezone.utc)

    conn = psycopg2.connect(cfg.postgres_dsn)
    try:
        reference_rows = _load_features(
            conn, now - timedelta(days=REFERENCE_DAYS), now - timedelta(days=CURRENT_DAYS)
        )
        current_rows = _load_features(conn, now - timedelta(days=CURRENT_DAYS), now)
    finally:
        conn.close()

    if len(reference_rows) < MIN_WINDOW_ROWS or len(current_rows) < MIN_WINDOW_ROWS:
        raise SystemExit(
            f"not enough predictions for drift: reference={len(reference_rows)}, "
            f"current={len(current_rows)} (need >= {MIN_WINDOW_ROWS} in each window)"
        )

    reference = _as_frame(reference_rows)
    current = _as_frame(current_rows)
    log.info(
        "Drift windows: reference=%d rows, current=%d rows",
        len(reference),
        len(current),
    )

    report = Report([DataDriftPreset()])
    snapshot = report.run(
        current_data=Dataset.from_pandas(current, data_definition=DATA_DEFINITION),
        reference_data=Dataset.from_pandas(reference, data_definition=DATA_DEFINITION),
    )

    share = extract_drift_share(snapshot)

    day = now.strftime("%Y-%m-%d")
    out_dir = Path(os.environ.get("DRIFT_REPORT_DIR", "/tmp/drift_reports")) / day
    out_dir.mkdir(parents=True, exist_ok=True)
    html_path = out_dir / "report.html"
    json_path = out_dir / "report.json"
    snapshot.save_html(str(html_path))
    snapshot.save_json(str(json_path))

    for path, name, content_type in (
        (html_path, "report.html", "text/html"),
        (json_path, "report.json", "application/json"),
    ):
        uri = write_bytes(path.read_bytes(), f"drift_reports/{day}/{name}", content_type)
        log.info("Uploaded → %s", uri)

    DRIFT_SHARE.set(share)
    push_to_gateway(cfg.pushgateway_url, job="drift", registry=REGISTRY)
    log.info("Published evidently_drift_share=%.4f", share)

    return share


if __name__ == "__main__":
    print(f"drift_share={run():.4f}")
