"""Prediction endpoints."""
from __future__ import annotations
import time
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from api.model_loader import get_model_info
from api.shadow import ShadowRouter
from api.metrics import predictions_total, prediction_latency, fraud_prob_gauge
from training.feature_engineering import engineer_features, get_model_input_frame
import pandas as pd

router = APIRouter(prefix="/predict", tags=["predict"])
shadow_router = ShadowRouter()


class TransactionIn(BaseModel):
    transaction_id: str
    timestamp:      str
    user_id:        str
    merchant_id:    str
    merchant_cat:   str = Field(..., examples=["electronics"])
    amount_usd:     float
    currency:       str = "USD"
    country:        str = "US"
    device_type:    str = Field(..., examples=["mobile"])
    ip_hash:        str = "00000000"
    card_last4:     str = "0000"
    is_international: bool = False
    hour_of_day:    int = 12
    day_of_week:    int = 0


class PredictionOut(BaseModel):
    transaction_id:   str
    fraud_probability: float
    decision:          str
    model_version:     str
    shadow_prob:       float | None
    latency_ms:        float


@router.post("/", response_model=PredictionOut)
async def predict(tx: TransactionIn):
    t0 = time.perf_counter()
    info = get_model_info()
    version = str(info.get("version", "unknown"))

    row = pd.DataFrame([tx.model_dump()])
    row = engineer_features(row)
    features = get_model_input_frame(row).values[0]

    try:
        result = shadow_router.predict(features, version)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"model unavailable: {exc}") from exc

    latency_ms = (time.perf_counter() - t0) * 1000

    prediction_latency.observe(latency_ms / 1000)
    predictions_total.labels(decision=result["decision"], model_version=version).inc()
    fraud_prob_gauge.set(result["champion_prob"])

    return PredictionOut(
        transaction_id=tx.transaction_id,
        fraud_probability=result["champion_prob"],
        decision=result["decision"],
        model_version=version,
        shadow_prob=result["shadow_prob"],
        latency_ms=round(latency_ms, 2),
    )


@router.get("/model/info")
async def model_info():
    return get_model_info()
