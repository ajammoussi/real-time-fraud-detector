"""Ingestion status endpoint."""
from __future__ import annotations

import json
import subprocess
import sys

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/ingest", tags=["ingestion"])


@router.get("/status")
async def ingest_status() -> dict:
	"""Return lake and ingestion health as reported by ingestion_ctl."""
	proc = subprocess.run(
		[sys.executable, "scripts/ingestion_ctl.py", "status"],
		capture_output=True,
		text=True,
		check=False,
	)
	if proc.returncode != 0:
		raise HTTPException(
			status_code=503,
			detail=proc.stderr.strip() or "ingestion status unavailable",
		)

	try:
		return json.loads(proc.stdout)
	except json.JSONDecodeError as exc:
		raise HTTPException(status_code=500, detail=f"invalid status payload: {exc}") from exc
