"""Unit tests for FastAPI endpoints using TestClient."""

from unittest.mock import patch

from fastapi.testclient import TestClient

from api.main import app
from api.model_loader import ModelUnavailable

client = TestClient(app)

PAYLOAD = {
    "transaction_id": "txn_test001",
    "timestamp": "2024-06-01T12:00:00Z",
    "user_id": "usr_00001",
    "merchant_id": "mrch_0001",
    "merchant_cat": "electronics",
    "amount_usd": 49.99,
    "currency": "USD",
    "country": "US",
    "device_type": "mobile",
    "ip_hash": "aabb1122",
    "card_last4": "1234",
    "is_international": False,
    "hour_of_day": 14,
    "day_of_week": 2,
}

CHAMPION = {
    "fraud_probability": 0.03,
    "decision": "APPROVE",
    "model_version": "1",
    "threshold": 0.5,
    "model_features": {"amount_usd": 49.99},
}


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


@patch("api.routers.predict.record_prediction", return_value=True)
@patch("api.routers.predict.shadow_router.predict")
def test_predict_approve(mock_shadow, mock_record):
    mock_shadow.return_value = {"champion": CHAMPION, "challenger": None}

    r = client.post("/predict/", json=PAYLOAD)

    assert r.status_code == 200
    data = r.json()
    assert set(data) == {
        "transaction_id",
        "fraud_probability",
        "decision",
        "model_version",
        "shadow_probability",
        "latency_ms",
    }
    assert data["decision"] == "APPROVE"
    assert data["model_version"] == "1"
    assert data["shadow_probability"] is None
    assert 0.0 <= data["fraud_probability"] <= 1.0

    stored = mock_record.call_args.kwargs
    assert stored["transaction_id"] == "txn_test001"
    assert stored["decision"] == "APPROVE"
    assert stored["features"] == CHAMPION["model_features"]
    assert stored["shadow_prob"] is None


@patch("api.routers.predict.record_prediction", return_value=True)
@patch("api.routers.predict.shadow_router.predict")
def test_predict_reports_shadow_probability(mock_shadow, mock_record):
    mock_shadow.return_value = {
        "champion": CHAMPION,
        "challenger": {"prob": 0.42, "decision": "APPROVE"},
    }

    data = client.post("/predict/", json=PAYLOAD).json()

    assert data["shadow_probability"] == 0.42
    assert mock_record.call_args.kwargs["shadow_prob"] == 0.42


@patch("api.routers.predict.shadow_router.predict")
def test_predict_model_unavailable_is_503(mock_shadow):
    mock_shadow.side_effect = ModelUnavailable("no Production model")

    r = client.post("/predict/", json=PAYLOAD)

    assert r.status_code == 503
    assert "model unavailable" in r.json()["detail"]


@patch("api.routers.predict.record_prediction", side_effect=RuntimeError("pg down"))
@patch("api.routers.predict.shadow_router.predict")
def test_predict_survives_database_failure(mock_shadow, mock_record):
    mock_shadow.return_value = {"champion": CHAMPION, "challenger": None}

    r = client.post("/predict/", json=PAYLOAD)

    assert r.status_code == 200
    assert r.json()["decision"] == "APPROVE"


@patch("api.routers.predict.record_prediction", return_value=True)
@patch("api.routers.predict.shadow_router.predict")
def test_metrics_is_served_without_a_redirect(mock_shadow, mock_record):
    mock_shadow.return_value = {"champion": CHAMPION, "challenger": None}
    client.post("/predict/", json=PAYLOAD)

    r = client.get("/metrics")

    assert r.status_code == 200
    assert r.history == [], "a redirect would leave the scrape URL empty"
    assert "predictions_total" in r.text
