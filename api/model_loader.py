"""Load the served model from the MLflow registry, refreshed on a TTL.

A promoted model must reach the API without a restart, so the cached entry is
re-validated against the registry every `model_cache_ttl_secs`. Re-validation is
a cheap metadata read; the model artifact is only re-downloaded when the
version or run actually changed.
"""

from __future__ import annotations

import time
from typing import Any

import mlflow
from mlflow.tracking import MlflowClient

from config.settings import get_settings


class ModelUnavailable(RuntimeError):
    """No model version is servable for the requested stage."""


# stage → (monotonic cached_at, model, registry info for that exact version)
_cache: dict[str, tuple[float, Any, dict]] = {}
# stage → (version, run_id) currently held in _cache
_identity: dict[str, tuple[str, str]] = {}


def invalidate_cache() -> None:
    """Drop every cached model so the next call re-reads the registry."""
    _cache.clear()
    _identity.clear()


def get_model_info(stage: str | None = None) -> dict:
    """Registry metadata for the model serving *stage*."""
    cfg = get_settings()
    target_stage = stage or cfg.model_stage
    client = MlflowClient(cfg.mlflow_tracking_uri)
    versions = client.get_latest_versions(cfg.model_name, stages=[target_stage])
    if not versions:
        return {
            "model_name": cfg.model_name,
            "stage": target_stage,
            "version": None,
            "run_id": None,
            "created_at": None,
            "decision_threshold": cfg.decision_threshold,
        }

    version = versions[0]
    tag = version.tags.get("decision_threshold") if version.tags else None
    return {
        "model_name": cfg.model_name,
        "stage": target_stage,
        "version": version.version,
        "run_id": version.run_id,
        "created_at": version.creation_timestamp,
        "decision_threshold": float(tag) if tag else cfg.decision_threshold,
    }


def load_model_and_info(stage: str | None = None) -> tuple[Any, dict]:
    """Return (model, info) for *stage*, refreshing both when the cache TTL lapses.

    The metadata is cached alongside the artifact on purpose: version and
    decision_threshold must describe the model that actually produces the score.
    Reading the registry separately would let a freshly promoted version be
    stamped on predictions the previous artifact scored, for up to one TTL.
    """
    cfg = get_settings()
    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)

    target_stage = stage or cfg.model_stage
    cached = _cache.get(target_stage)
    now = time.monotonic()

    if cached and now - cached[0] <= cfg.model_cache_ttl_secs:
        return cached[1], cached[2]

    try:
        info = get_model_info(target_stage)
    except Exception as exc:
        raise ModelUnavailable(
            f"registry unreachable for {cfg.model_name}/{target_stage}: {exc}"
        ) from exc

    if info["version"] is None:
        raise ModelUnavailable(f"{cfg.model_name} has no version in stage {target_stage}")

    identity = (str(info["version"]), str(info["run_id"]))
    if cached and _identity.get(target_stage) == identity:
        # Same artifact as before — renew the timestamp and pick up tag changes.
        _cache[target_stage] = (now, cached[1], info)
        return cached[1], info

    invalidate_cache()
    try:
        model = mlflow.sklearn.load_model(f"models:/{cfg.model_name}/{target_stage}")
    except Exception as exc:
        raise ModelUnavailable(
            f"could not load {cfg.model_name}/{target_stage} v{info['version']}: {exc}"
        ) from exc
    _cache[target_stage] = (now, model, info)
    _identity[target_stage] = identity
    return model, info


def load_model(stage: str | None = None):
    """Return the model for *stage*, refreshing it once the cache TTL lapses."""
    return load_model_and_info(stage)[0]
