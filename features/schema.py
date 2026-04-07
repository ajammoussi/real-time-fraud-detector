"""Names of the velocity features — the contract shared by every stage.

Kept free of any Feast import so training, drift detection and serving can all
agree on the feature names without dragging the feature-store runtime along.
"""

from __future__ import annotations

USER_FEATURES = [
    "user_txn_count_7d",
    "user_amount_sum_7d",
    "user_amount_avg_7d",
    "user_amount_max_7d",
]
MERCHANT_FEATURES = ["merchant_txn_count_7d", "merchant_amount_avg_7d"]
VELOCITY_FEATURES = USER_FEATURES + MERCHANT_FEATURES
