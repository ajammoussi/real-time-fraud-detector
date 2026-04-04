"""Initial schema for prediction and model performance tables.

Revision ID: 20260404_0001
Revises:
Create Date: 2026-04-04 00:00:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260404_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())

    if "predictions" not in existing_tables:
        op.create_table(
            "predictions",
            sa.Column("id", sa.BigInteger(), sa.Identity(always=False), primary_key=True, nullable=False),
            sa.Column("transaction_id", sa.Text(), nullable=False),
            sa.Column("model_version", sa.Text(), nullable=False),
            sa.Column("fraud_prob", sa.Float(), nullable=False),
            sa.Column("decision", sa.Text(), nullable=False),
            sa.Column("features_json", sa.JSON(), nullable=True),
            sa.Column("shadow_prob", sa.Float(), nullable=True),
            sa.Column("latency_ms", sa.Float(), nullable=True),
            sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.CheckConstraint("decision IN ('APPROVE','REJECT')", name="ck_predictions_decision"),
        )
        op.create_index("idx_predictions_created_at", "predictions", ["created_at"])
        op.create_index("idx_predictions_model_ver", "predictions", ["model_version"])
        op.create_index("idx_predictions_decision", "predictions", ["decision"])

    if "model_performance_log" not in existing_tables:
        op.create_table(
            "model_performance_log",
            sa.Column("id", sa.BigInteger(), sa.Identity(always=False), primary_key=True, nullable=False),
            sa.Column("model_version", sa.Text(), nullable=False),
            sa.Column("date", sa.Date(), nullable=False),
            sa.Column("auc_roc", sa.Float(), nullable=True),
            sa.Column("f1", sa.Float(), nullable=True),
            sa.Column("avg_prec", sa.Float(), nullable=True),
            sa.Column("drift_score", sa.Float(), nullable=True),
            sa.Column("logged_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        )


def downgrade() -> None:
    op.drop_table("model_performance_log")
    op.drop_index("idx_predictions_decision", table_name="predictions")
    op.drop_index("idx_predictions_model_ver", table_name="predictions")
    op.drop_index("idx_predictions_created_at", table_name="predictions")
    op.drop_table("predictions")
