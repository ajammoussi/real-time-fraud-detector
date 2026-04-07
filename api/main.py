"""FastAPI application entry point."""

from __future__ import annotations

import logging
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from api.routers import ingest_status, predict

log = logging.getLogger("api")


def _warm_up() -> None:
    """Load the model and feature store ahead of the first request."""
    from api.model_loader import load_model
    from features.store import get_store

    for name, loader in (("model", load_model), ("feature store", get_store)):
        try:
            loader()
        except Exception as exc:
            # Startup must not fail: the routes report unavailability per request.
            log.warning("%s warm-up failed: %s", name, exc)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    threading.Thread(target=_warm_up, name="warm-up", daemon=True).start()
    yield


app = FastAPI(title="Fraud Detection MLOps API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(predict.router)
app.include_router(ingest_status.router)


@app.get("/metrics")
def metrics() -> Response:
    """Prometheus scrape endpoint (no trailing-slash redirect).

    The API runs a single uvicorn worker on purpose: each worker owns its own
    registry, so multiple workers make every counter flap between processes.
    """
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health():
    return {"status": "ok"}
