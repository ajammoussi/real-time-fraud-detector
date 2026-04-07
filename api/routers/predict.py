"""Prediction endpoints."""

from __future__ import annotations

import logging
import time

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from api.metrics import fraud_prob_gauge, prediction_latency, predictions_total
from api.model_loader import ModelUnavailable, get_model_info
from api.shadow import ShadowRouter
from db.writer import record_prediction

log = logging.getLogger("api.predict")

router = APIRouter(prefix="/predict", tags=["predict"])
shadow_router = ShadowRouter()


class TransactionIn(BaseModel):
    transaction_id: str
    timestamp: str
    user_id: str
    merchant_id: str
    merchant_cat: str = Field(..., examples=["electronics"])
    amount_usd: float
    currency: str = "USD"
    country: str = "US"
    device_type: str = Field(..., examples=["mobile"])
    ip_hash: str = "00000000"
    card_last4: str = "0000"
    is_international: bool = False
    hour_of_day: int = 12
    day_of_week: int = 0
    # Accepted so a lake record can be replayed through the API unchanged.
    # It is never used: a request must not be able to supply its own label.
    label: int | None = None


class PredictionOut(BaseModel):
    transaction_id: str
    fraud_probability: float
    decision: str
    model_version: str
    shadow_probability: float | None
    latency_ms: float


@router.post("/", response_model=PredictionOut)
def predict(tx: TransactionIn):
    """Score one transaction.

    Declared sync on purpose: FastAPI runs sync handlers in its threadpool, so
    the blocking MLflow/pandas/psycopg2 work never occupies the event loop.
    """
    started = time.perf_counter()
    try:
        result = shadow_router.predict(tx.model_dump())
    except ModelUnavailable as exc:
        raise HTTPException(status_code=503, detail=f"model unavailable: {exc}") from exc

    champion = result["champion"]
    challenger = result["challenger"]
    shadow_probability = challenger["prob"] if challenger else None
    latency_ms = (time.perf_counter() - started) * 1000

    prediction_latency.observe(latency_ms / 1000)
    predictions_total.labels(
        decision=champion["decision"], model_version=champion["model_version"]
    ).inc()
    fraud_prob_gauge.set(champion["fraud_probability"])

    try:
        record_prediction(
            transaction_id=tx.transaction_id,
            model_version=champion["model_version"],
            fraud_prob=champion["fraud_probability"],
            decision=champion["decision"],
            features=champion["model_features"],
            shadow_prob=shadow_probability,
            latency_ms=latency_ms,
        )
    except Exception as exc:
        # A database hiccup must not fail an otherwise valid prediction.
        log.warning("prediction audit insert failed for %s: %s", tx.transaction_id, exc)

    return PredictionOut(
        transaction_id=tx.transaction_id,
        fraud_probability=champion["fraud_probability"],
        decision=champion["decision"],
        model_version=champion["model_version"],
        shadow_probability=shadow_probability,
        latency_ms=round(latency_ms, 2),
    )


@router.get("/model/info")
def model_info():
    return get_model_info()
