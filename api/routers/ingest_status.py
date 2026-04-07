"""Ingestion status endpoint."""

from __future__ import annotations

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, HTTPException

from datalake.storage import lake_status

router = APIRouter(prefix="/ingest", tags=["ingestion"])


@router.get("/status")
def ingest_status() -> dict:
    """Return lake and ingestion health, read directly from the data lake."""
    try:
        return lake_status()
    except (BotoCoreError, ClientError, OSError) as exc:
        raise HTTPException(status_code=503, detail=f"data lake unreachable: {exc}") from exc
