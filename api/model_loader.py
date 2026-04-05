"""Load the production model from MLflow registry."""

from __future__ import annotations

import mlflow
from mlflow.tracking import MlflowClient

from config.settings import get_settings

_model_cache: dict = {}


def load_model(stage: str | None = None):
    cfg = get_settings()
    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    target_stage = stage or cfg.model_stage
    cache_key = f"{cfg.model_name}:{target_stage}"
    if cache_key not in _model_cache:
        model_uri = f"models:/{cfg.model_name}/{target_stage}"
        _model_cache[cache_key] = mlflow.sklearn.load_model(model_uri)
    return _model_cache[cache_key]


def get_model_info(stage: str | None = None) -> dict:
    cfg = get_settings()
    client = MlflowClient(cfg.mlflow_tracking_uri)
    target_stage = stage or cfg.model_stage
    versions = client.get_latest_versions(cfg.model_name, stages=[target_stage])
    if not versions:
        return {"model_name": cfg.model_name, "stage": target_stage, "version": None}
    v = versions[0]
    return {
        "model_name": cfg.model_name,
        "stage": target_stage,
        "version": v.version,
        "run_id": v.run_id,
        "created_at": v.creation_timestamp,
    }


def invalidate_cache():
    _model_cache.clear()
