"""Central configuration.

All settings come from environment variables (optionally loaded from a `.env`
file at the repository root). No credentials are hard-coded anywhere.
"""

from __future__ import annotations

import os
import platform
from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parent.parent

# Transactional / event datasets that flow bronze -> silver -> gold
DATASETS = (
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "inventory_events",
    "shipping_events",
)
# Slowly changing reference data delivered through the reference API / catalog feeds
REFERENCE_DATASETS = ("regions", "exchange_rates", "categories", "suppliers", "warehouses")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=REPO_ROOT / ".env", env_file_encoding="utf-8", extra="ignore")

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "dataforge"
    postgres_user: str = "dataforge"
    postgres_password: str = ""

    # Lake
    dataforge_data_dir: Path = Field(default=REPO_ROOT / "data")

    # Spark
    spark_master: str = "local[*]"
    spark_driver_memory: str = "2g"
    spark_shuffle_partitions: int = 8
    spark_tmp_dir: str = ""
    spark_writer: str = ""  # "native" | "arrow" | "" (auto-detect)

    # Sources
    reference_api_url: str = "file://./data/raw/api"

    # Generation
    dataforge_seed: int = 20240101
    dataforge_scale: float = 1.0

    # Serving
    api_port: int = 8000
    dashboard_port: int = 8501
    dbt_target: str = "dev"

    # ------------------------------------------------------------------ paths
    @property
    def data_dir(self) -> Path:
        p = Path(self.dataforge_data_dir)
        return p if p.is_absolute() else (REPO_ROOT / p).resolve()

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.data_dir / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.data_dir / "gold"

    @property
    def quarantine_dir(self) -> Path:
        return self.data_dir / "quarantine"

    @property
    def monitoring_dir(self) -> Path:
        return self.data_dir / "monitoring"

    # -------------------------------------------------------------- database
    @property
    def postgres_dsn(self) -> str:
        pw = f" password={self.postgres_password}" if self.postgres_password else ""
        return (
            f"host={self.postgres_host} port={self.postgres_port} dbname={self.postgres_db} "
            f"user={self.postgres_user}{pw}"
        )

    def redacted_dsn(self) -> str:
        return f"postgresql://{self.postgres_user}:***@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    # ----------------------------------------------------------------- spark
    @property
    def effective_spark_tmp_dir(self) -> str:
        if self.spark_tmp_dir:
            return self.spark_tmp_dir
        if platform.system() == "Windows":
            # JDK unix-domain-socket pipes fail on temp paths containing spaces; keep it short
            # and on the same drive as the repository (Spark spills can be large).
            drive = REPO_ROOT.drive or "C:"
            return f"{drive}/tmp/dataforge"
        return "/tmp/dataforge"

    @property
    def hadoop_natives_available(self) -> bool:
        """True when Spark can use the local filesystem natively.

        On Linux/macOS this is always true. On Windows, Hadoop needs winutils.exe/hadoop.dll
        (pointed to by HADOOP_HOME) for local directory listing and file writes.
        """
        if platform.system() != "Windows":
            return True
        home = os.environ.get("HADOOP_HOME")
        return bool(home) and (Path(home) / "bin" / "winutils.exe").exists()

    @property
    def effective_spark_writer(self) -> str:
        if self.spark_writer in ("native", "arrow"):
            return self.spark_writer
        return "native" if self.hadoop_natives_available else "arrow"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def reset_settings_cache() -> None:
    """Used by tests that mutate environment variables."""
    get_settings.cache_clear()
