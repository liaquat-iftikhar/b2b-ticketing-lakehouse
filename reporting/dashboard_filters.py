from typing import List, Tuple

import duckdb
import streamlit as st

from reporting.duckdb_model import DuckDBModel


class DashboardFilters:
    """Sidebar filter state for the sales dashboard."""

    def __init__(self, model: DuckDBModel):
        self.model = model
        self.selected_event_types: List[str] = []
        self.selected_resellers: List[str] = []

    def render_sidebar(self) -> None:
        """Render multiselect widgets and update the current filter state."""
        st.sidebar.header("Data Warehouse Filters")

        event_opts, reseller_opts = self._fetch_dimensions(self.model.warehouse_path)

        self.selected_event_types = st.sidebar.multiselect(
            "Filter by Event Types", options=event_opts, default=event_opts
        )
        self.selected_resellers = st.sidebar.multiselect(
            "Filter by Resellers / Channels",
            options=reseller_opts,
            default=reseller_opts,
        )

    @staticmethod
    @st.cache_data(ttl=60)
    def _fetch_dimensions(path: str) -> Tuple[List[str], List[str]]:
        """
        Load distinct event types and resellers from the Delta table.

        Cached as a static method to avoid serializing the DuckDBModel instance,
        which Streamlit cannot hash. The connection is closed after each call.
        """
        con = duckdb.connect(database=":memory:")
        try:
            con.execute("INSTALL delta; LOAD delta;")
            event_types = (
                con.execute(
                    f"SELECT DISTINCT event_type FROM delta_scan('{path}') WHERE event_type IS NOT NULL"
                )
                .fetchdf()["event_type"]
                .tolist()
            )
            resellers = (
                con.execute(
                    f"SELECT DISTINCT COALESCE(reseller_id, 'Direct Platform Sale') AS reseller"
                    f" FROM delta_scan('{path}')"
                )
                .fetchdf()["reseller"]
                .tolist()
            )
        finally:
            con.close()
        return event_types, resellers

    def get_filter(self) -> Tuple[str, List]:
        """
        Build a parameterized WHERE clause from current selections.

        Returns a (sql_fragment, params) tuple. The SQL uses positional '?'
        placeholders so values are never interpolated directly into the query
        string, preventing SQL injection.
        """
        if not self.selected_event_types or not self.selected_resellers:
            return "WHERE 1=0", []

        params: List = []
        conditions: List[str] = []

        # Event type filter
        placeholders = ", ".join("?" * len(self.selected_event_types))
        conditions.append(f"event_type IN ({placeholders})")
        params.extend(self.selected_event_types)

        # Reseller filter — "Direct Platform Sale" is a display sentinel for NULL reseller_id
        named_resellers = [r for r in self.selected_resellers if r != "Direct Platform Sale"]
        reseller_parts: List[str] = []

        if named_resellers:
            rp = ", ".join("?" * len(named_resellers))
            reseller_parts.append(f"reseller_id IN ({rp})")
            params.extend(named_resellers)

        if "Direct Platform Sale" in self.selected_resellers:
            reseller_parts.append("reseller_id IS NULL")

        if not reseller_parts:
            return "WHERE 1=0", []

        conditions.append(f"({' OR '.join(reseller_parts)})")

        return "WHERE " + " AND ".join(conditions), params
