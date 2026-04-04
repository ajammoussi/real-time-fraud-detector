"""Locust load test for /predict endpoint."""
from locust import HttpUser, task, between
import random, uuid

MERCHANT_CATS = ["electronics","grocery","travel","clothing","restaurants"]
DEVICE_TYPES  = ["mobile","desktop","tablet","pos_terminal"]


class PredictUser(HttpUser):
    wait_time = between(0.05, 0.2)

    @task
    def predict(self):
        payload = {
            "transaction_id": f"txn_{uuid.uuid4().hex[:8]}",
            "timestamp": "2024-06-15T14:32:07Z",
            "user_id": f"usr_{random.randint(1, 50000):05d}",
            "merchant_id": f"mrch_{random.randint(1, 5000):04d}",
            "merchant_cat": random.choice(MERCHANT_CATS),
            "amount_usd": round(random.uniform(1.0, 2000.0), 2),
            "currency": "USD",
            "country": "US",
            "device_type": random.choice(DEVICE_TYPES),
            "ip_hash": "aabb1122",
            "card_last4": "1234",
            "is_international": False,
            "hour_of_day": random.randint(0, 23),
            "day_of_week": random.randint(0, 6),
        }
        self.client.post("/predict/", json=payload)
