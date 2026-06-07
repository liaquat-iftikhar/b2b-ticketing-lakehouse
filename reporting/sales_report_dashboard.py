import os
import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px


# ==============================================================================
# 1. MODEL LAYER: DATA ENGINE & REPOSITORY
# ==============================================================================
class WarehouseModel:
    """Encapsulates the DuckDB core engine context and file-system query executions."""

    def __init__(self, warehouse_path: str):
        self.warehouse_path = warehouse_path
        self._validate_storage_path()
        self.connection = self._get_cached_engine()

    def _validate_storage_path(self):
        """Verifies physical presence of the target data warehouse folder."""
        if not os.path.exists(self.warehouse_path):
            st.error(f"Target Data Warehouse path not found: `{self.warehouse_path}`")
            st.info("Please verify that your upstream PySpark Gold pipelines have run successfully.")
            st.stop()

    @staticmethod
    @st.cache_resource
    def _get_cached_engine():
        """Bootstraps a single shared in-memory connection context with Delta extensions."""
        con = duckdb.connect(database=':memory:')
        con.execute("INSTALL delta; LOAD delta;")
        return con

    def run_query(self, query: str) -> pd.DataFrame:
        """Executes analytical SQL and streams the payload back into a standard DataFrame."""
        return self.connection.execute(query).fetchdf()


# ==============================================================================
# 2. CONTROLLER LAYER: FILTER COMPONENT
# ==============================================================================
class DashboardFilters:
    """Manages the state, widgets, and parsing logic for dynamic runtime query parameters."""

    def __init__(self, model: WarehouseModel):
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


# ==============================================================================
# 3. VIEW LAYER: PRESENTATION & VISUALIZATION PANELS
# ==============================================================================
class SalesDashboardView:
    """Orchestrates layout configuration, widget placement, and charting canvas bindings."""

    def __init__(self, model: WarehouseModel, filters: DashboardFilters):
        self.model = model
        self.filters = filters

    def render_app_layout(self):
        """Main rendering pipeline sequence executing page structures sequentially."""
        st.set_page_config(page_title="Corporate Sales Data Warehouse", page_icon="", layout="wide")
        st.title("Corporate Sales Data Warehouse Analytics")
        st.markdown("### Open-Source Serverless Data Lakehouse Reporting Engine")
        st.write("---")

        self.filters.render_sidebar()
        filter_sql = self.filters.get_where_clause()

        self._render_workspace_tabs(filter_sql)
        self._render_ledger_table(filter_sql)

    def _render_workspace_tabs(self, filter_sql: str):
        """Constructs partitioned analysis areas targeting discrete client demands."""
        tab1, tab2, tab3, tab4 = st.tabs([
            "Weekly Channel Trends",
            "YoY February Comparison",
            "Commission Correlation",
            "Regional Popularity"
        ])

        with tab1:
            st.subheader("Weekly Sales Volume Segmented by Channel")
            df_weekly = self.model.run_query(f"""
                SELECT 
                    EXTRACT(WEEK FROM CAST(sale_date AS DATE)) as week_number,
                    UPPER(sales_channel) as channel,
                    SUM(total_amount) as gross_sales
                FROM delta_scan('{self.model.warehouse_path}')
                {filter_sql}
                GROUP BY 1, 2 ORDER BY week_number ASC
            """)
            if not df_weekly.empty:
                fig = px.line(df_weekly, x="week_number", y="gross_sales", color="channel", markers=True)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No timeline records found matching filter constraints.")

        with tab2:
            st.subheader("⚖️ YoY February Evaluation Matrix")
            df_feb = self.model.run_query(f"""
                SELECT 
                    EXTRACT(YEAR FROM CAST(sale_date AS DATE)) as sale_year,
                    SUM(total_amount) as gross_revenue
                FROM delta_scan('{self.model.warehouse_path}')
                {filter_sql}
                AND EXTRACT(MONTH FROM CAST(sale_date AS DATE)) = 2
                AND EXTRACT(YEAR FROM CAST(sale_date AS DATE)) IN (2019, 2020)
                GROUP BY 1
                ORDER BY sale_year ASC
            """)
            if not df_feb.empty and 2020 in df_feb['sale_year'].values:
                rev_19 = df_feb[df_feb['sale_year'] == 2019]['gross_revenue'].values[0] if 2019 in df_feb[
                    'sale_year'].values else 0
                rev_20 = df_feb[df_feb['sale_year'] == 2020]['gross_revenue'].values[0] if 2020 in df_feb[
                    'sale_year'].values else 0
                delta = ((rev_20 - rev_19) / rev_19 * 100) if rev_19 > 0 else 0

                st.metric("February 2020 Revenue Total", f"${rev_20:,.2f}", delta=f"{delta:+.2f}% vs 2019 baseline")
                fig = px.bar(df_feb, x="sale_year", y="gross_revenue", color="sale_year")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Baseline data points for comparison windows are unavailable within current filters.")

        with tab3:
            st.subheader("Commission Rate Performance Correlation Analysis")
            df_comm = self.model.run_query(f"""
                SELECT CAST(commission_rate_applied AS DOUBLE) as commission_rate, total_amount as transaction_value, tickets_count as quantity_sold
                FROM delta_scan('{self.model.warehouse_path}') {filter_sql}
            """)

            if not df_comm.empty:
                scatter_kwargs = {
                    "data_frame": df_comm,
                    "x": "commission_rate",
                    "y": "transaction_value",
                    "size": "quantity_sold",
                    "labels": {"commission_rate": "Incentive Commission Bracket (%)",
                               "transaction_value": "Logged Gross Value ($)"},
                    "title": "Regression Distribution: Commission Structure Influence Matrix"
                }

                scatter_kwargs["trendline"] = "ols"
                scatter_kwargs["trendline_color_override"] = "#D62728"

                fig = px.scatter(**scatter_kwargs)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Incentive transaction metrics are empty for the current parameters.")

        with tab4:
            st.subheader("Geographic Demand: Most Popular Tickets per Region")
            df_popular = self.model.run_query(f"""
                WITH aggregated_regional_sales AS (
                    SELECT UPPER(event_region) as region, event_name as event, SUM(tickets_count) as total_tickets_sold
                    FROM delta_scan('{self.model.warehouse_path}') {filter_sql}
                    GROUP BY UPPER(event_region), event_name
                )
                SELECT region, event, total_tickets_sold FROM aggregated_regional_sales
                QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_tickets_sold DESC) <= 3
            """)
            if not df_popular.empty:
                df_popular['event'] = df_popular['event'].str.title()
                fig = px.bar(df_popular, x="total_tickets_sold", y="region", color="event", orientation="h",
                             barmode="group")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No spatial data points identified.")

    def _render_ledger_table(self, filter_sql: str):
        """Displays interactive scrolling rows and configures downstream CSV extract targets."""
        st.write("---")
        st.subheader("Data Warehouse Ledger Export")

        df_ledger = self.model.run_query(f"""
            SELECT sale_id, event_name, ticket_category, tickets_count as quantity, total_amount, commission_amount, sales_channel, source
            FROM delta_scan('{self.model.warehouse_path}') {filter_sql} ORDER BY total_amount DESC
        """)

        if not df_ledger.empty:
            df_ledger['event_name'] = df_ledger['event_name'].str.title()
            df_ledger['sales_channel'] = df_ledger['sales_channel'].str.upper()
            df_ledger['source'] = df_ledger['source'].str.replace('_', ' ').str.title()

            st.dataframe(
                df_ledger, use_container_width=True, hide_index=True,
                column_config={
                    "total_amount": st.column_config.NumberColumn("Total Amount", format="$%.2f"),
                    "commission_amount": st.column_config.NumberColumn("Commission Amount", format="$%.2f"),
                }
            )

            st.download_button(
                label="Download Conformed Data Warehouse Audit Spreadsheet (.CSV)",
                data=df_ledger.to_csv(index=False).encode('utf-8'),
                file_name="conformed_data_warehouse_sales_export.csv",
                mime="text/csv"
            )
        else:
            st.warning("No ledger entries compiled for current selection parameters.")


# ==============================================================================
# 4. RUNTIME SYSTEM APPLICATION BOOTSTRAPPER
# ==============================================================================
if __name__ == "__main__":
    # Point straight to your target workspace layer folder
    TARGET_PATH = "/Volumes/Work/learning/Iftikhar-liaquat/data/lakehouse/sales_report_etl/gold"

    # Instantiate Object Infrastructure Lifecycles
    dw_model = WarehouseModel(warehouse_path=TARGET_PATH)
    ui_filters = DashboardFilters(model=dw_model)
    dashboard_app = SalesDashboardView(model=dw_model, filters=ui_filters)

    # Execute Presentation Build Routine
    dashboard_app.render_app_layout()
