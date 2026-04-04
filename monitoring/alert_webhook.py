"""Webhook receiver: Alertmanager → GitHub Actions retrain dispatch."""
from __future__ import annotations
import httpx
from fastapi import FastAPI, Request
from config.settings import get_settings

app   = FastAPI(title="Alert Webhook")
cfg   = get_settings()


@app.post("/webhook/alert")
async def receive_alert(request: Request):
    body = await request.json()
    alerts = body.get("alerts", [])
    for alert in alerts:
        name   = alert.get("labels", {}).get("alertname", "")
        status = alert.get("status", "")
        if status == "firing" and name in ("FraudDriftHigh", "ModelAUCLow"):
            await _trigger_retrain(reason=name)
    return {"status": "ok"}


async def _trigger_retrain(reason: str):
    url = (f"https://api.github.com/repos/{cfg.github_owner}/"
           f"{cfg.github_repo}/actions/workflows/retrain.yml/dispatches")
    headers = {"Authorization": f"Bearer {cfg.github_token}",
               "Accept": "application/vnd.github+json"}
    payload = {"ref": "main", "inputs": {"reason": reason}}
    async with httpx.AsyncClient() as client:
        r = await client.post(url, json=payload, headers=headers)
        r.raise_for_status()
    print(f"Retrain triggered via GitHub Actions (reason={reason})")
