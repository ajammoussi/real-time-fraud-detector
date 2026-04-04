"""Shadow mode / A/B testing router."""
from __future__ import annotations
import numpy as np
from config.settings import get_settings
from api.model_loader import load_model


class ShadowRouter:
    def __init__(self):
        self.cfg = get_settings()

    def predict(self, features: np.ndarray, model_version: str) -> dict:
        champion = load_model("Production")
        champion_prob = float(champion.predict_proba(features.reshape(1, -1))[0, 1])

        shadow_prob = None
        if self.cfg.shadow_traffic_fraction >= np.random.random():
            try:
                challenger = load_model("Staging")
                shadow_prob = float(
                    challenger.predict_proba(features.reshape(1, -1))[0, 1]
                )
            except Exception:
                pass  # challenger not available — silent fail

        return {
            "champion_prob": champion_prob,
            "shadow_prob":   shadow_prob,
            "decision":      "REJECT" if champion_prob >= 0.5 else "APPROVE",
        }
