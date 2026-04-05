"""Unit tests for FastAPI endpoints using TestClient."""

from unittest.mock import patch

from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


@patch("api.routers.predict.get_model_info", return_value={"version": "1"})
@patch(
    "api.routers.predict.shadow_router.predict",
    return_value={"champion_prob": 0.03, "shadow_prob": None, "decision": "APPROVE"},
)
def test_predict_approve(mock_shadow, mock_info):
    payload = {
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
    r = client.post("/predict/", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["decision"] == "APPROVE"
    assert 0.0 <= data["fraud_probability"] <= 1.0
