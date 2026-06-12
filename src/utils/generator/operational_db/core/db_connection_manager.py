import psycopg2

from src.config.app_config import DatabaseConfig


class DatabaseConnectionManager:
    """
    Manages psycopg2 transaction scope and guarantees safe resource cleanup.
    Accepts the centralized DatabaseConfig so credentials come from one source.
    """

    def __init__(self, config: DatabaseConfig) -> None:
        self._config = config
        self._connection = None

    def __enter__(self):
        self._connection = psycopg2.connect(
            host=self._config.host,
            port=self._config.port,
            dbname=self._config.name,
            user=self._config.user,
            password=self._config.password,
        )
        self._connection.autocommit = False
        return self._connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._connection:
            if exc_type is not None:
                self._connection.rollback()
                print(
                    f"Error encountered — transaction rolled back.\nDetails: {exc_val}"
                )
            else:
                self._connection.commit()
                print("All data committed successfully to the database.")
            self._connection.close()
