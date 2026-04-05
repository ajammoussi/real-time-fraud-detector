"""Daily drift detection using Evidently AI."""

from __future__ import annotations

import json
import os
from datetime import date, timedelta

import boto3
import pandas as pd
import psycopg2
from evidently import ColumnMapping
from evidently.metric_preset import DataDriftPreset
from evidently.report import Report

from config.settings import get_settings

cfg = get_settings()


def load_reference_data() -> pd.DataFrame:
    """Load training data from SeaweedFS as reference distribution."""
    import s3fs

    fs = s3fs.S3FileSystem(
        key=cfg.seaweed_access_key,
        secret=cfg.seaweed_secret_key,
        endpoint_url=cfg.seaweed_endpoint,
    )
    files = sorted(fs.glob(f"{cfg.datalake_bucket}/raw/*.parquet"), reverse=True)
    if not files:
        raise ValueError("No raw parquet files found for reference distribution")
    dfs = [pd.read_parquet(f"s3://{f}", filesystem=fs) for f in files[:3]]
    combined = pd.concat(dfs, ignore_index=True)
    n = min(5000, len(combined))
    return combined.sample(n=n, random_state=42) if n else combined


def load_current_data(days_back: int = 1) -> pd.DataFrame:
    """Pull yesterday's predictions + features from PostgreSQL."""
    since = date.today() - timedelta(days=days_back)
    conn = psycopg2.connect(cfg.postgres_dsn)
    df = pd.read_sql_query(
        """
        SELECT features_json, fraud_prob, decision
        FROM predictions
        WHERE created_at >= %s::date
        """,
        conn,
        params=[str(since)],
    )
    conn.close()
    if df.empty:
        raise ValueError(f"No predictions found since {since}")
    features = pd.json_normalize(df["features_json"])
    features["fraud_prob"] = df["fraud_prob"].values
    features["prediction"] = (df["fraud_prob"] >= 0.5).astype(int).values
    return features


def run_drift_report():
    print("Loading reference data …")
    ref = load_reference_data()
    print("Loading current data …")
    cur = load_current_data()

    col_map = ColumnMapping(
        numerical_features=["amount_usd", "hour_of_day", "day_of_week"],
        categorical_features=["merchant_cat", "device_type", "is_international"],
    )

    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=ref, current_data=cur, column_mapping=col_map)

    today = date.today().strftime("%Y/%m/%d")
    out_dir = f"/tmp/drift_reports/{today}"
    os.makedirs(out_dir, exist_ok=True)
    html_path = f"{out_dir}/report.html"
    json_path = f"{out_dir}/report.json"
    report.save_html(html_path)
    report.save_json(json_path)

    # Upload to SeaweedFS
    s3 = boto3.client(
        "s3",
        endpoint_url=cfg.seaweed_endpoint,
        aws_access_key_id=cfg.seaweed_access_key,
        aws_secret_access_key=cfg.seaweed_secret_key,
    )
    for local, s3_key in [
        (html_path, f"drift_reports/{today}/report.html"),
        (json_path, f"drift_reports/{today}/report.json"),
    ]:
        s3.upload_file(local, cfg.datalake_bucket, s3_key)
        print(f"Uploaded → s3://{cfg.datalake_bucket}/{s3_key}")

    result = report.as_dict()
    drift_score = None
    for metric in result.get("metrics", []):
        values = metric.get("result", {})
        drift_score = values.get("drift_share", values.get("dataset_drift_share"))
        if drift_score is not None:
            break

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    print(f"Drift score: {drift_score}")
    return drift_score


if __name__ == "__main__":
    run_drift_report()
