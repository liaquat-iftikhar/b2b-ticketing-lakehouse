import os
from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class JDBCPartitionConfig:
    column: str
    lower_bound: str
    upper_bound: str
    num_partitions: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "column": self.column,
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "num_partitions": self.num_partitions
        }


class DBConnectionManager:
    """
    Handles secure credential acquisition and connection parameter management.
    Abstracts environment differences between local Docker and Cloud Orchestrators.
    """
    def __init__(self) -> None:
        # DE Best Practice: Never hardcode secrets. Read from environment with fallback defaults.
        self._user = os.getenv("DB_USER", "postgres")
        self._password = os.getenv("DB_PASSWORD", "postgres")
        self._host = os.getenv("DB_HOST", "localhost")
        self._port = os.getenv("DB_PORT", "5432")
        self._db_name = os.getenv("DB_NAME", "event_platform")
        self._driver = os.getenv("DB_DRIVER", "org.postgresql.Driver")

    @property
    def jdbc_url(self) -> str:
        """Dynamically constructs the destination host connection string string."""
        return f"jdbc:postgresql://{self._host}:{self._port}/{self._db_name}"

    @property
    def db_properties(self) -> Dict[str, str]:
        """Compiles standard driver execution properties safely wrapper."""
        return {
            "user": self._user,
            "password": self._password,
            "driver": self._driver
        }
