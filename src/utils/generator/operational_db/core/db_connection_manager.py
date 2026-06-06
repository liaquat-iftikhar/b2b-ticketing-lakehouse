import os
from dataclasses import dataclass, field
import psycopg2


@dataclass(frozen=True)
class DatabaseConfig:
    """Immutable data container for DB parameters extracted from environment variables."""
    host: str = field(default_factory=lambda: os.getenv("DB_HOST", "localhost"))
    port: str = field(default_factory=lambda: os.getenv("DB_PORT", "5432"))
    name: str = field(default_factory=lambda: os.getenv("DB_NAME", "event_platform"))
    user: str = field(default_factory=lambda: os.getenv("DB_USER", "postgres"))
    password: str = field(default_factory=lambda: os.getenv("DB_PASSWORD", "postgres"))


class DatabaseConnectionManager:
    """Manages transaction state scope and handles infrastructure cleanups safely."""
    def __init__(self, config: DatabaseConfig):
        self._config = config
        self._connection = None

    def __enter__(self):
        self._connection = psycopg2.connect(
            host=self._config.host,
            port=self._config.port,
            dbname=self._config.name,
            user=self._config.user,
            password=self._config.password
        )
        self._connection.autocommit = False
        return self._connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._connection:
            if exc_type is not None:
                self._connection.rollback()
                print(f"Error encountered — Transaction rolled back.\nDetails: {exc_val}")
            else:
                self._connection.commit()
                print("All data committed successfully to the database.")
            self._connection.close()