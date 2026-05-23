from functools import lru_cache
from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    app_name: str = "crypto-fastapi"

    # Kafka
    kafka_bootstrap: str = "kafka:9092"
    kafka_topic: str = "crypto.klines"

    # Redis
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: str = "redis123"

    # MongoDB
    mongo_uri: str = "mongodb://admin:admin123@mongodb:27017"
    mongo_db: str = "crypto_db"
    mongo_collection_ohlc: str = "ohlc_5m"

    # CORS
    cors_origins: List[str] = ["*"]

    # Symbols
    symbols: List[str] = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
        "ADAUSDT", "XRPUSDT", "DOGEUSDT", "AVAXUSDT",
    ]

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
