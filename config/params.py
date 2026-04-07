"""Typed access to params.yaml — the single source of pipeline parameters."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import BaseModel

_ROOT_DIR = Path(__file__).resolve().parents[1]
_PARAMS_FILE = _ROOT_DIR / "params.yaml"


class ModelParams(BaseModel):
    n_trials: int
    n_estimators_min: int
    n_estimators_max: int
    max_depth_max: int
    learning_rate_min: float
    learning_rate_max: float
    early_stopping_rounds: int
    min_auc: float
    min_avg_prec: float


class DatasetParams(BaseModel):
    max_files: int
    max_rows: int
    min_labeled_rows: int


class Params(BaseModel):
    model: ModelParams
    dataset: DatasetParams


@lru_cache
def get_params() -> Params:
    """Load and validate params.yaml (cached for the life of the process)."""
    if not _PARAMS_FILE.exists():
        raise FileNotFoundError(f"params file not found: {_PARAMS_FILE}")
    raw = yaml.safe_load(_PARAMS_FILE.read_text(encoding="utf-8")) or {}
    return Params.model_validate(raw)
