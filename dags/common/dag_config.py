"""
Central DAG configuration module.

All Airflow connection IDs and data paths are sourced from the centralized
AppConfig (Dynaconf-backed settings.toml + env var overrides).

Every DAG should import from here instead of hardcoding values locally:
"""

import os
import sys
from pathlib import Path

from src.config.config_loader import ConfigLoader

_config = ConfigLoader.load()

SPARK_CONN_ID: str = _config.airflow.spark_conn_id
POSTGRES_CONN_ID: str = _config.airflow.postgres_conn_id
LAKEHOUSE_PATH: str = _config.storage.base_path
INBOUND_PATH: str = _config.storage.inbound_path
PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]
SRC_JOBS_ROOT: Path = PROJECT_ROOT / "src" / "jobs"
PYSPARK_PYTHON: str = os.getenv("PYSPARK_PYTHON", sys.executable)
PYSPARK_DRIVER_PYTHON: str = os.getenv("PYSPARK_DRIVER_PYTHON", sys.executable)
