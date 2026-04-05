"""Gradio demo UI for HuggingFace Spaces."""

import os

import gradio as gr
import httpx

API_URL = os.getenv("FRAUD_API_URL", "http://localhost:8000")


def predict(
    transaction_id, amount_usd, merchant_cat, device_type, hour_of_day, is_international
):
    payload = {
        "transaction_id": transaction_id or "txn_demo",
        "timestamp": "2024-06-15T14:32:07Z",
        "user_id": "usr_00001",
        "merchant_id": "mrch_0001",
        "merchant_cat": merchant_cat,
        "amount_usd": float(amount_usd),
        "currency": "USD",
        "country": "US",
        "device_type": device_type,
        "ip_hash": "aabb1122",
        "card_last4": "1234",
        "is_international": is_international,
        "hour_of_day": int(hour_of_day),
        "day_of_week": 2,
    }
    try:
        r = httpx.post(f"{API_URL}/predict/", json=payload, timeout=10)
        r.raise_for_status()  # raise on 4xx/5xx status codes
        data = r.json()
        prob = data.get("fraud_probability")
        if prob is None:
            return "API error: missing fraud_probability in response", "—", "—"
        label = f"{'🚨 FRAUD' if prob > 0.5 else '✅ LEGIT'} ({prob:.1%})"
        model_ver = data.get("model_version", "unknown")
        latency = data.get("latency_ms", 0)
        return label, model_ver, f"{latency:.1f} ms"
    except httpx.HTTPStatusError as e:
        return f"API error {e.response.status_code}: {e.response.text[:100]}", "—", "—"
    except Exception as e:
        return f"Error: {type(e).__name__}: {str(e)[:80]}", "—", "—"


demo = gr.Interface(
    fn=predict,
    inputs=[
        gr.Textbox(label="Transaction ID", value="txn_abc123"),
        gr.Number(label="Amount (USD)", value=49.99),
        gr.Dropdown(["electronics", "grocery", "travel", "clothing"], label="Category"),
        gr.Dropdown(["mobile", "desktop", "tablet"], label="Device"),
        gr.Slider(0, 23, value=14, label="Hour of Day"),
        gr.Checkbox(label="International?"),
    ],
    outputs=[
        gr.Textbox(label="Verdict"),
        gr.Textbox(label="Model Version"),
        gr.Textbox(label="Latency"),
    ],
    title="🔍 Fraud Detection Demo",
    description="Real-time fraud scoring powered by the MLOps pipeline",
)

if __name__ == "__main__":
    # Allow override of server bind and port from environment (useful in containers)
    server_name = os.getenv("GRADIO_SERVER_NAME", "0.0.0.0")
    server_port = int(os.getenv("GRADIO_SERVER_PORT", "7860"))
    demo.launch(server_name=server_name, server_port=server_port)
