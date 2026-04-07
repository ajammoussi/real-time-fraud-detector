"""Centralised, validated configuration via Pydantic BaseSettings."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_ROOT_DIR = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=_ROOT_DIR / ".env", extra="ignore")

    # SeaweedFS / S3
    seaweed_access_key: str = "minioadmin"
    seaweed_secret_key: str = "minioadmin"
    seaweed_endpoint: str = "http://localhost:8333"
    datalake_bucket: str = "datalake"

    # MLflow
    mlflow_tracking_uri: str = "http://localhost:5000"
    mlflow_s3_endpoint_url: str = "http://localhost:8333"
    mlflow_experiment_name: str = "fraud_detection"

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "mlops"
    postgres_user: str = "mlops"
    postgres_password: str = "mlops_secret"

    @property
    def postgres_dsn(self) -> str:
        """libpq DSN for psycopg2 (`psycopg2.connect`, drift, prediction writer)."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def postgres_sqlalchemy_url(self) -> str:
        """SQLAlchemy URL for Alembic.

        The driver is pinned: a bare `postgresql://` lets SQLAlchemy choose
        psycopg (v3) on recent versions, which the images do not install.
        """
        return self.postgres_dsn.replace("postgresql://", "postgresql+psycopg2://", 1)

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_transactions_raw: str = "transactions_raw"  # raw feed from Binance WebSocket
    kafka_consumer_group: str = "prediction-workers"
    kafka_consumer_group_lake: str = "lake-writers"  # ingestion consumer group

    # Binance WebSocket feed (public, no auth required)
    # Docs: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
    binance_ws_base: str = "wss://stream.binance.com:9443/stream"
    binance_symbols: str = "btcusdt,ethusdt,bnbusdt,solusdt,xrpusdt"  # comma-sep

    # Streaming-to-lake ingestion settings
    lake_batch_size: int = 1_000  # flush after N messages
    lake_batch_timeout_secs: int = 300  # OR after N seconds, whichever first

    # Model serving
    model_name: str = "fraud_detector"
    model_stage: str = "Production"
    challenger_stage: str = "Staging"
    model_cache_ttl_secs: int = 60
    decision_threshold: float = Field(0.5, ge=0.0, le=1.0)
    shadow_traffic_fraction: float = Field(1.0, ge=0.0, le=1.0)

    # Features (Feast)
    feature_repo_path: str = "feature_repo"
    feature_stats_prefix: str = "features"

    # Monitoring
    pushgateway_url: str = "http://pushgateway:9091"
    alert_webhook_token: str = ""

    # Consumers
    consumer_metrics_port: int = 9101

    # GitHub
    github_token: str = ""
    github_owner: str = ""
    github_repo: str = ""


@lru_cache
def get_settings() -> Settings:
    return Settings()
