from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict
from dynaconf import Dynaconf

class Environment(str, Enum):
    LOCAL      = "local"
    PRODUCTION = "production"


class StorageType(str, Enum):
    LOCAL = "local"
    S3    = "s3"


@dataclass(frozen=True)
class SparkConfig:
    app_name:           str
    shuffle_partitions: int                = 100
    extra_configs:      Dict[str, str]     = field(default_factory=dict)


@dataclass(frozen=True)
class StorageConfig:
    type:         StorageType
    base_path:    str
    inbound_path: str = ""

    @property
    def is_s3(self) -> bool:
        return self.type == StorageType.S3

    def layer_path(self, layer: str, dataset: str) -> str:
        return f"{self.base_path.rstrip('/')}/{dataset}/{layer.lower()}"


@dataclass(frozen=True)
class DatabaseConfig:
    host:     str
    port:     int
    name:     str
    user:     str
    password: str
    driver:   str = "org.postgresql.Driver"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.name}"

    @property
    def jdbc_properties(self) -> Dict[str, str]:
        return {
            "user":     self.user,
            "password": self.password,
            "driver":   self.driver,
        }


@dataclass(frozen=True)
class AirflowConfig:
    """Airflow connection IDs — names must match connections registered in Airflow."""
    spark_conn_id:    str = "spark_local"
    postgres_conn_id: str = "postgres_event_platform"


@dataclass(frozen=True)
class AppConfig:
    environment: Environment
    spark:       SparkConfig
    storage:     StorageConfig
    database:    DatabaseConfig
    airflow:     AirflowConfig = field(default_factory=AirflowConfig)


_PROJECT_ROOT = Path(__file__).parents[2]

_dynaconf = Dynaconf(
    root_path=str(_PROJECT_ROOT),
    envvar_prefix="DYNACONF",
    settings_files=["config/settings.toml", "config/.secrets.toml"],
    environments=True,
    env_switcher="ENV_FOR_DYNACONF",
    default_env="default",
    env="local",
    load_dotenv=True,
    dotenv_path=".env",
    nested_match=True,
)
