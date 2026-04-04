"""SQLAlchemy models for Alembic autogenerate.

Define the tables used by the application so Alembic can autogenerate migrations.
"""
from sqlalchemy import (
    Column,
    BigInteger,
    Text,
    Float,
    JSON,
    TIMESTAMP,
    Date,
    func,
    CheckConstraint,
    Index,
    Identity,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Prediction(Base):
    __tablename__ = "predictions"
    __table_args__ = (
        CheckConstraint("decision IN ('APPROVE','REJECT')", name="ck_predictions_decision"),
        Index("idx_predictions_created_at", "created_at"),
        Index("idx_predictions_model_ver", "model_version"),
        Index("idx_predictions_decision", "decision"),
    )

    id = Column(BigInteger, Identity(always=False), primary_key=True)
    transaction_id = Column(Text, nullable=False)
    model_version = Column(Text, nullable=False)
    fraud_prob = Column(Float, nullable=False)
    decision = Column(Text, nullable=False)
    features_json = Column(JSON, nullable=True)
    shadow_prob = Column(Float, nullable=True)
    latency_ms = Column(Float, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)


class ModelPerformanceLog(Base):
    __tablename__ = "model_performance_log"

    id = Column(BigInteger, Identity(always=False), primary_key=True)
    model_version = Column(Text, nullable=False)
    date = Column(Date, nullable=False)
    auc_roc = Column(Float, nullable=True)
    f1 = Column(Float, nullable=True)
    avg_prec = Column(Float, nullable=True)
    drift_score = Column(Float, nullable=True)
    logged_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
