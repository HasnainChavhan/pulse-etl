"""
PulseETL — Core Configuration
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "PulseETL"
    app_version: str = "1.0.0"
    debug: bool = False

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "pulse.events"
    kafka_consumer_group: str = "pulse-etl-consumer"
    kafka_auto_offset_reset: str = "earliest"
    kafka_batch_size: int = 500

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/pulsetl"
    db_pool_size: int = 10

    anomaly_std_threshold: float = 3.0
    dedup_window_seconds: int = 60

    class Config:
        env_file = ".env"


settings = Settings()
