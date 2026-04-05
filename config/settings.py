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
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_transactions_raw: str = (
        "transactions_raw"  # raw feed from Binance WebSocket
    )
    kafka_topic_transactions: str = "transactions"  # validated, schema-mapped records
    kafka_topic_predictions: str = "predictions"
    kafka_consumer_group: str = "prediction-workers"
    kafka_consumer_group_lake: str = "lake-writers"  # ingestion consumer group

    # Binance WebSocket feed (public, no auth required)
    # Docs: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
    binance_ws_base: str = "wss://stream.binance.com:9443/stream"
    binance_symbols: str = "btcusdt,ethusdt,bnbusdt,solusdt,xrpusdt"  # comma-sep

    # Streaming-to-lake ingestion settings
    lake_batch_size: int = 1_000  # flush after N messages
    lake_batch_timeout_secs: int = 300  # OR after N seconds, whichever first

    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    jwt_secret: str = "change_me_in_prod"
    shadow_traffic_fraction: float = Field(1.0, ge=0.0, le=1.0)
    model_name: str = "fraud_detector"
    model_stage: str = "Production"

    # GitHub
    github_token: str = ""
    github_owner: str = ""
    github_repo: str = ""


@lru_cache
def get_settings() -> Settings:
    return Settings()
