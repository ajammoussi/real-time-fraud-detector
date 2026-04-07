"""Champion/challenger scoring with a deterministic, TTL-backed sampler.

The challenger is scored on a configurable fraction of traffic. When its stage
has no servable version, the router stops retrying the registry for
`model_cache_ttl_secs` instead of hitting MLflow on every request.
"""

from __future__ import annotations

import logging
import random
import time

from api.inference import score
from api.model_loader import ModelUnavailable, load_model_and_info
from config.settings import get_settings

log = logging.getLogger("api.shadow")


class ShadowRouter:
    def __init__(self):
        self.cfg = get_settings()
        self._challenger_missing_until = 0.0

    def predict(self, transaction: dict) -> dict:
        """Score *transaction* as champion, plus the challenger when sampled in."""
        champion = score(transaction)

        challenger = None
        now = time.monotonic()
        if (
            self.cfg.shadow_traffic_fraction > 0
            and now >= self._challenger_missing_until
            and random.random() < self.cfg.shadow_traffic_fraction
        ):
            try:
                challenger_model, challenger_info = load_model_and_info(self.cfg.challenger_stage)
                challenger_result = score(transaction, model=challenger_model, info=challenger_info)
                challenger = {
                    "prob": challenger_result["fraud_probability"],
                    "decision": challenger_result["decision"],
                }
            except ModelUnavailable as exc:
                self._challenger_missing_until = time.monotonic() + self.cfg.model_cache_ttl_secs
                log.warning(
                    "challenger stage %s unavailable — pausing shadow scoring for %ds: %s",
                    self.cfg.challenger_stage,
                    self.cfg.model_cache_ttl_secs,
                    exc,
                )

        return {"champion": champion, "challenger": challenger}
