"""Make prediction writes idempotent on transaction_id.

Revision ID: 20260407_0002
Revises: 20260404_0001
Create Date: 2026-04-07 10:20:00
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260407_0002"
down_revision = "20260404_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Rows written before this migration may already hold duplicates; keep the
    # oldest of each transaction_id so the constraint can be created.
    op.execute(
        "DELETE FROM predictions p USING predictions q "
        "WHERE p.id > q.id AND p.transaction_id = q.transaction_id"
    )
    op.create_unique_constraint("uq_predictions_transaction_id", "predictions", ["transaction_id"])


def downgrade() -> None:
    op.drop_constraint("uq_predictions_transaction_id", "predictions", type_="unique")
