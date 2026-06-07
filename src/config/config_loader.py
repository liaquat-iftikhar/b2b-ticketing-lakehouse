from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict

from dynaconf import Dynaconf

from app_config import AppConfig, Environment, _dynaconf, SparkConfig, StorageConfig, StorageType, DatabaseConfig


class ConfigLoader:
    """
    Reads the active Dynaconf settings and returns a fully typed AppConfig.
    """

    @classmethod
    def load(cls) -> AppConfig:
        return AppConfig(
            environment = Environment(_dynaconf.current_env.lower()),
            spark       = cls._build_spark(),
            storage     = cls._build_storage(),
            database    = cls._build_database(),
        )

    # ------------------------------------------------------------------
    # Private builders
    # ------------------------------------------------------------------

    @classmethod
    def _build_spark(cls) -> SparkConfig:
        s = _dynaconf.spark
        return SparkConfig(
            app_name           = s.app_name,
            shuffle_partitions = int(s.shuffle_partitions),
            extra_configs      = dict(s.get("extra_configs") or {}),
        )

    @classmethod
    def _build_storage(cls) -> StorageConfig:
        s = _dynaconf.storage
        return StorageConfig(
            type      = StorageType(s.type),
            base_path = s.base_path,
        )

    @classmethod
    def _build_database(cls) -> DatabaseConfig:
        d = _dynaconf.database
        return DatabaseConfig(
            host     = d.host,
            port     = int(d.port),
            name     = d.name,
            user     = d.user,
            password = d.password,
            driver   = d.get("driver", "org.postgresql.Driver"),
        )
