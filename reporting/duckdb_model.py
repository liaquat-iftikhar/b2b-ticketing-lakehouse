import os
from typing import List, Optional

import duckdb
import pandas as pd
import streamlit as st


class DuckDBModel:
    """Shared DuckDB connection for querying Delta Lake files."""

    def __init__(self, warehouse_path: str):
        self.warehouse_path = warehouse_path
        self._validate_storage_path()
        self.connection = self._get_cached_engine(warehouse_path)

    def _validate_storage_path(self) -> None:
        if not os.path.exists(self.warehouse_path):
            st.error(f"Warehouse path not found: `{self.warehouse_path}`")
            st.info("Verify that the upstream PySpark Gold pipeline has completed.")
            st.stop()

    @staticmethod
    @st.cache_resource
    def _get_cached_engine(warehouse_path: str) -> duckdb.DuckDBPyConnection:
        """
        Create one in-memory DuckDB connection per distinct warehouse_path.
        """
        con = duckdb.connect(database=":memory:")
        con.execute("INSTALL delta; LOAD delta;")
        return con

    def run_query(self, query: str, params: Optional[List] = None) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame.

        Args:
            query: SQL string, optionally with '?' positional placeholders.
            params: Values to bind to the placeholders in order.
        """
        return self.connection.execute(query, params or []).fetchdf()
