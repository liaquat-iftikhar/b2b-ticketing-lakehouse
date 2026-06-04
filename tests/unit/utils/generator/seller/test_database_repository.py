import unittest
from unittest.mock import patch, MagicMock
import psycopg2

from src.utils.generator.seller.database_repository import DatabaseRepository, DatabaseConfig


class TestDatabaseRepository(unittest.TestCase):
    """Unit tests for DatabaseRepository verify connection lifecycle management and error propagation."""

    def setUp(self) -> None:
        self.config = DatabaseConfig(
            host="localhost",
            database="test_db",
            user="test_user",
            password="test_password",
            port=5432
        )
        self.repo = DatabaseRepository(connection_config=self.config)

    @patch("src.utils.generator.seller.database_repository.psycopg2.connect")
    def test_execute_schema_initialization_success(self, mock_connect) -> None:
        """Verifies successful schema initialization calls execute and closes the connection."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        self.repo.execute_schema_initialization()

        mock_connect.assert_called_once_with(**self.config.to_dict())
        mock_cursor.execute.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("src.utils.generator.seller.database_repository.psycopg2.connect")
    def test_execute_schema_initialization_failure(self, mock_connect) -> None:
        """Verifies schema initialization raises error and still closes the connection on failure."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.execute.side_effect = psycopg2.DatabaseError("Syntax Error")

        with self.assertRaises(psycopg2.Error):
            self.repo.execute_schema_initialization()

        mock_conn.close.assert_called_once()

    @patch("src.utils.generator.seller.database_repository.psycopg2.connect")
    def test_batch_insert_entities_success(self, mock_connect) -> None:
        """Verifies batch inserts execute successfully, and close the connection."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        table = "test_table"
        query = "INSERT INTO test_table VALUES (%s);"
        data = [("value1",), ("value2",)]

        self.repo.batch_insert_entities(table=table, query=query, data=data)

        mock_cursor.executemany.assert_called_once_with(query, data)
        mock_conn.close.assert_called_once()

    @patch("src.utils.generator.seller.database_repository.psycopg2.connect")
    def test_batch_insert_entities_failure(self, mock_connect) -> None:
        """Verifies batch inserts raise error and close the connection on database failure."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.executemany.side_effect = psycopg2.IntegrityError("Constraint violation")

        table = "test_table"
        query = "INSERT INTO test_table VALUES (%s);"
        data = [("value1",), ("value2",)]

        with self.assertRaises(psycopg2.Error):
            self.repo.batch_insert_entities(table=table, query=query, data=data)

        mock_conn.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
