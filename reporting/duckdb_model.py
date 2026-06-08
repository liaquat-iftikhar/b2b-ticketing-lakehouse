import os
import duckdb
import pandas as pd
import streamlit as st


class DuckDBModel:
    """Encapsulates the DuckDB core engine context and file-system query executions."""

    def __init__(self, warehouse_path: str):
        self.warehouse_path = warehouse_path
        self._validate_storage_path()
        self.connection = self._get_cached_engine()

    def _validate_storage_path(self):
        """Verifies physical presence of the target data warehouse folder."""
        if not os.path.exists(self.warehouse_path):
            st.error(f"Target Data Warehouse path not found: `{self.warehouse_path}`")
            st.info(
                "Please verify that your upstream PySpark Gold pipelines have run successfully."
            )
            st.stop()

    @staticmethod
    @st.cache_resource
    def _get_cached_engine():
        """Bootstraps a single shared in-memory connection context with Delta extensions."""
        con = duckdb.connect(database=":memory:")
        con.execute("INSTALL delta; LOAD delta;")
        return con

    def run_query(self, query: str) -> pd.DataFrame:
        """Executes analytical SQL and streams the payload back into a standard DataFrame."""
        return self.connection.execute(query).fetchdf()
