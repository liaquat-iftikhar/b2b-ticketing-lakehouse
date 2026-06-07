import duckdb
import streamlit as st

from reporting.duckdb_model import DuckDBModel


class DashboardFilters:
    """Manages the state, widgets, and parsing logic for dynamic runtime query parameters."""

    def __init__(self, model: DuckDBModel):
        self.model = model
        self.selected_event_types = []
        self.selected_resellers = []

    def render_sidebar(self):
        """Builds interactive navigation selectors in the Streamlit Sidebar container."""
        st.sidebar.header("Data Warehouse Filters")

        # Load unique dimensions dynamically using static safe cache blocks
        event_opts, reseller_opts = self._fetch_dimensions(self.model.warehouse_path)

        self.selected_event_types = st.sidebar.multiselect(
            "Filter by Event Types", options=event_opts, default=event_opts
        )
        self.selected_resellers = st.sidebar.multiselect(
            "Filter by Resellers / Channels", options=reseller_opts, default=reseller_opts
        )

    @staticmethod
    @st.cache_data(ttl=60)
    def _fetch_dimensions(path: str):
        """Static cached query extractor protecting instance serialization tracking loops."""
        con = duckdb.connect(database=':memory:')
        con.execute("INSTALL delta; LOAD delta;")

        event_types = \
        con.execute(f"SELECT DISTINCT event_type FROM delta_scan('{path}') WHERE event_type IS NOT NULL").fetchdf()[
            'event_type'].tolist()
        resellers = con.execute(
            f"SELECT DISTINCT COALESCE(reseller_id, 'Direct Platform Sale') as reseller FROM delta_scan('{path}')").fetchdf()[
            'reseller'].tolist()
        return event_types, resellers

    def get_where_clause(self) -> str:
        """Compiles selections into valid SQL, preventing exceptions when filters are unset."""

        if not self.selected_event_types or not self.selected_resellers:
            return "WHERE 1=0"

        where_clause = "WHERE 1=1"

        if len(self.selected_event_types) == 1:
            where_clause += f" AND event_type = '{self.selected_event_types[0]}'"
        else:
            where_clause += f" AND event_type IN {tuple(self.selected_event_types)}"

        reseller_filter = [r for r in self.selected_resellers if r != 'Direct Platform Sale']
        conditions = []

        if reseller_filter:
            if len(reseller_filter) == 1:
                conditions.append(f"reseller_id = '{reseller_filter[0]}'")
            else:
                conditions.append(f"reseller_id IN {tuple(reseller_filter)}")

        if 'Direct Platform Sale' in self.selected_resellers:
            conditions.append("reseller_id IS NULL")

        if conditions:
            where_clause += f" AND ({' OR '.join(conditions)})"
        else:
            return "WHERE 1=0"

        return where_clause
