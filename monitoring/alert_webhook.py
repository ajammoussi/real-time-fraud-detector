"""Webhook receiver: Alertmanager → GitHub Actions retrain dispatch.

Fails loudly instead of silently dropping alerts: an unauthenticated call gets
401, and a request that cannot be turned into a GitHub dispatch gets 503 naming
the missing configuration.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request

from config.settings import get_settings

log = logging.getLogger("alert-webhook")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

cfg = get_settings()

# Alerts that should trigger a retrain. ModelAUCLow is gone: there are no labels
# at serve time, so a live AUC cannot be computed.
RETRAIN_ALERTS = ("FraudDriftHigh",)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    if not cfg.alert_webhook_token:
        log.warning("ALERT_WEBHOOK_TOKEN is empty — the webhook accepts unauthenticated calls")
    if not (cfg.github_owner and cfg.github_repo and cfg.github_token):
        log.warning(
            "GITHUB_OWNER/GITHUB_REPO/GITHUB_TOKEN incomplete — retrain dispatch will answer 503"
        )
    yield


app = FastAPI(title="Alert Webhook", lifespan=lifespan)


def _authorized(request: Request) -> bool:
    if not cfg.alert_webhook_token:
        return True
    return request.headers.get("authorization") == f"Bearer {cfg.alert_webhook_token}"


async def _trigger_retrain(reason: str) -> str:
    workflow_url = (
        f"https://github.com/{cfg.github_owner}/{cfg.github_repo}/actions/workflows/retrain.yml"
    )
    url = (
        f"https://api.github.com/repos/{cfg.github_owner}/{cfg.github_repo}"
        "/actions/workflows/retrain.yml/dispatches"
    )
    headers = {
        "Authorization": f"Bearer {cfg.github_token}",
        "Accept": "application/vnd.github+json",
    }
    payload = {"ref": "main", "inputs": {"reason": reason}}
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=payload, headers=headers)
        response.raise_for_status()

    # A workflow dispatch answers 204 with no body; prefer the Location header
    # when GitHub provides one, otherwise point at the workflow page.
    run_url = response.headers.get("Location", workflow_url)
    log.info("Retrain dispatched (reason=%s): %s", reason, run_url)
    return run_url


@app.post("/webhook/alert")
async def receive_alert(request: Request):
    if not _authorized(request):
        raise HTTPException(status_code=401, detail="invalid webhook token")

    if not (cfg.github_owner and cfg.github_repo and cfg.github_token):
        raise HTTPException(
            status_code=503,
            detail=("retrain dispatch is not configured (GITHUB_OWNER/GITHUB_REPO/GITHUB_TOKEN)"),
        )

    body = await request.json()
    dispatched = []
    for alert in body.get("alerts", []):
        name = alert.get("labels", {}).get("alertname", "")
        if alert.get("status") == "firing" and name in RETRAIN_ALERTS:
            dispatched.append(await _trigger_retrain(reason=name))

    return {"status": "ok", "dispatched": dispatched}
