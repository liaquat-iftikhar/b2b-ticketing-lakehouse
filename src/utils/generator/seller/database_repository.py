import os
import logging
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Tuple

import psycopg2

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    database: str
    user: str
    password: str
    port: int = 5432

    def to_dict(self) -> dict:
        return asdict(self)


class DatabaseRepository:
    """
    Encapsulates all interaction behaviors targeting the storage engine.
    Implements a simplified Repository pattern for DDL initialization and batch saves.
    """
    def __init__(self, connection_config: DatabaseConfig) -> None:
        self._config = connection_config

    def execute_schema_initialization(self) -> None:
        """Executes source relational DDL structures exactly mapping parameters."""
        schema_path = Path(__file__).resolve().parent / "schema.sql"
        try:
            with open(schema_path, "r", encoding="utf-8") as f:
                ddl_script = f.read()
        except OSError as e:
            logger.error("Failed to read schema SQL file from %s: %s", schema_path, e)
            raise

        logger.info("Initializing database schema using %s...", schema_path)
        conn = None
        try:
            conn = psycopg2.connect(**self._config.to_dict())
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(ddl_script)
            logger.info("Database schema initialized successfully.")
        except psycopg2.Error as error:
            logger.error("Database schema initialization failed: %s", error)
            raise
        finally:
            if conn:
                conn.close()

    def batch_insert_entities(self, table: str, query: str, data: List[Tuple]) -> None:
        """Safely commits collections of data models in an atomic database transaction block."""
        logger.info("Batch inserting %d records into table '%s'...", len(data), table)
        conn = None
        try:
            conn = psycopg2.connect(**self._config.to_dict())
            with conn:
                with conn.cursor() as cursor:
                    cursor.executemany(query, data)
            logger.info("Successfully committed records to tracking collection: '%s'.", table)
        except psycopg2.Error as error:
            logger.error("Transaction rollback executed for target table '%s' due to database error: %s", table, error)
            raise
        finally:
            if conn:
                conn.close()