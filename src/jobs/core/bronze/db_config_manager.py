from dataclasses import dataclass
from typing import Dict

from src.config.app_config import DatabaseConfig


@dataclass(frozen=True)
class JDBCPartitionConfig:
    """Defines parallel read boundaries for a JDBC source table."""

    column: str
    lower_bound: str
    upper_bound: str
    num_partitions: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "column": self.column,
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "num_partitions": self.num_partitions,
        }


class DBConnectionManager:
    """
    Handles secure credential acquisition and connection parameter management.
    """

    def __init__(self, db_config: DatabaseConfig) -> None:
        self._config = db_config

    @property
    def jdbc_url(self) -> str:
        return self._config.jdbc_url

    @property
    def db_properties(self) -> Dict[str, str]:
        return self._config.jdbc_properties
