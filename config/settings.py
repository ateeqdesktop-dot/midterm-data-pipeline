"""Central configuration for the hybrid orders pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    mongo_uri: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_database: str = os.getenv("MONGO_DATABASE", "midterm_orders")
    small_file_threshold_mb: float = float(os.getenv("SMALL_FILE_THRESHOLD_MB", "200"))
    batch_size: int = int(os.getenv("BATCH_SIZE", "1000"))
    spark_partitions: int = int(os.getenv("SPARK_PARTITIONS", "4"))
    spark_master: str = os.getenv("SPARK_MASTER", "local[*]")
    spark_connector_package: str = os.getenv(
        "SPARK_MONGO_CONNECTOR_PACKAGE", "org.mongodb.spark:mongo-spark-connector_2.13:10.5.0"
    )
    timezone: str = os.getenv("PIPELINE_TIMEZONE", "UTC")

    def validate(self) -> None:
        if self.small_file_threshold_mb <= 0:
            raise ValueError("SMALL_FILE_THRESHOLD_MB must be positive")
        if self.batch_size <= 0 or self.spark_partitions <= 0:
            raise ValueError("BATCH_SIZE and SPARK_PARTITIONS must be positive")


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def get_settings() -> Settings:
    settings = Settings()
    settings.validate()
    return settings
