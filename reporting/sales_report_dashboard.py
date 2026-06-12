from typing import List, Tuple

import streamlit as st
import plotly.express as px

from reporting.dashboard_filters import DashboardFilters
from reporting.duckdb_model import DuckDBModel


class SalesDashboardView:
    """Renders the full sales dashboard: sidebar filters, charts, and the ledger table."""

    def __init__(self, model: DuckDBModel, filters: DashboardFilters):
        self.model = model
        self.filters = filters

    def render_app_layout(self) -> None:
        st.set_page_config(
            page_title="Corporate Sales Data Warehouse", page_icon="", layout="wide"
        )
        st.title("Corporate Sales Data Warehouse Analytics")
        st.markdown("### Open-Source Serverless Data Lakehouse Reporting Engine")
        st.write("---")

        self.filters.render_sidebar()
        filter_sql, filter_params = self.filters.get_filter()

        self._render_workspace_tabs(filter_sql, filter_params)
        self._render_ledger_table(filter_sql, filter_params)

    def _render_workspace_tabs(self, filter_sql: str, filter_params: List) -> None:
        tab1, tab2, tab3, tab4 = st.tabs(
            [
                "Weekly Channel Trends",
                "YoY February Comparison",
                "Commission Correlation",
                "Regional Popularity",
            ]
        )

        with tab1:
            st.subheader("Weekly Sales Volume Segmented by Channel")
            df_weekly = self.model.run_query(
                f"""
                SELECT
                    EXTRACT(WEEK FROM CAST(sale_date AS DATE)) AS week_number,
                    UPPER(sales_channel) AS channel,
                    SUM(total_amount) AS gross_sales
                FROM delta_scan('{self.model.warehouse_path}')
                {filter_sql}
                GROUP BY 1, 2
                ORDER BY week_number ASC
                """,
                filter_params,
            )
            if not df_weekly.empty:
                fig = px.line(
                    df_weekly,
                    x="week_number",
                    y="gross_sales",
                    color="channel",
                    markers=True,
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data found for the current filters.")

        with tab2:
            st.subheader("YoY February Comparison")
            df_feb = self.model.run_query(
                f"""
                SELECT
                    EXTRACT(YEAR FROM CAST(sale_date AS DATE)) AS sale_year,
                    SUM(total_amount) AS gross_revenue
                FROM delta_scan('{self.model.warehouse_path}')
                {filter_sql}
                AND EXTRACT(MONTH FROM CAST(sale_date AS DATE)) = 2
                AND EXTRACT(YEAR FROM CAST(sale_date AS DATE)) IN (2019, 2020)
                GROUP BY 1
                ORDER BY sale_year ASC
                """,
                filter_params,
            )
            if not df_feb.empty and 2020 in df_feb["sale_year"].values:
                rev_19 = (
                    df_feb[df_feb["sale_year"] == 2019]["gross_revenue"].values[0]
                    if 2019 in df_feb["sale_year"].values
                    else 0
                )
                rev_20 = df_feb[df_feb["sale_year"] == 2020]["gross_revenue"].values[0]
                delta = ((rev_20 - rev_19) / rev_19 * 100) if rev_19 > 0 else 0

                st.metric(
                    "February 2020 Revenue",
                    f"${rev_20:,.2f}",
                    delta=f"{delta:+.2f}% vs 2019",
                )
                fig = px.bar(df_feb, x="sale_year", y="gross_revenue", color="sale_year")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Not enough data for a year-over-year comparison.")

        with tab3:
            st.subheader("Commission Rate vs. Transaction Value")
            df_comm = self.model.run_query(
                f"""
                SELECT
                    CAST(commission_rate_applied AS DOUBLE) AS commission_rate,
                    total_amount AS transaction_value,
                    tickets_count AS quantity_sold
                FROM delta_scan('{self.model.warehouse_path}')
                {filter_sql}
                """,
                filter_params,
            )
            if not df_comm.empty:
                fig = px.scatter(
                    df_comm,
                    x="commission_rate",
                    y="transaction_value",
                    size="quantity_sold",
                    trendline="ols",
                    trendline_color_override="#D62728",
                    labels={
                        "commission_rate": "Commission Rate (%)",
                        "transaction_value": "Transaction Value ($)",
                    },
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data for the current filters.")

        with tab4:
            st.subheader("Top 3 Events by Ticket Volume per Region")
            df_popular = self.model.run_query(
                f"""
                WITH regional_sales AS (
                    SELECT
                        UPPER(event_region) AS region,
                        event_name AS event,
                        SUM(tickets_count) AS total_tickets_sold
                    FROM delta_scan('{self.model.warehouse_path}')
                    {filter_sql}
                    GROUP BY UPPER(event_region), event_name
                )
                SELECT region, event, total_tickets_sold
                FROM regional_sales
                QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_tickets_sold DESC) <= 3
                """,
                filter_params,
            )
            if not df_popular.empty:
                df_popular["event"] = df_popular["event"].str.title()
                fig = px.bar(
                    df_popular,
                    x="total_tickets_sold",
                    y="region",
                    color="event",
                    orientation="h",
                    barmode="group",
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No regional data for the current filters.")

    def _render_ledger_table(self, filter_sql: str, filter_params: List) -> None:
        st.write("---")
        st.subheader("Data Warehouse Ledger Export")

        df_ledger = self.model.run_query(
            f"""
            SELECT
                sale_id, event_name, ticket_category,
                tickets_count AS quantity, total_amount,
                commission_amount, sales_channel, source
            FROM delta_scan('{self.model.warehouse_path}')
            {filter_sql}
            ORDER BY total_amount DESC
            """,
            filter_params,
        )

        if not df_ledger.empty:
            df_ledger["event_name"] = df_ledger["event_name"].str.title()
            df_ledger["sales_channel"] = df_ledger["sales_channel"].str.upper()
            df_ledger["source"] = df_ledger["source"].str.replace("_", " ").str.title()

            st.dataframe(
                df_ledger,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "total_amount": st.column_config.NumberColumn(
                        "Total Amount", format="$%.2f"
                    ),
                    "commission_amount": st.column_config.NumberColumn(
                        "Commission Amount", format="$%.2f"
                    ),
                },
            )
            st.download_button(
                label="Download as CSV",
                data=df_ledger.to_csv(index=False).encode("utf-8"),
                file_name="sales_export.csv",
                mime="text/csv",
            )
        else:
            st.warning("No records match the current filters.")


if __name__ == "__main__":
    from src.config.config_loader import ConfigLoader

    _config = ConfigLoader.load()
    TARGET_PATH = _config.storage.layer_path("gold", "sales_report_etl")

    dw_model = DuckDBModel(warehouse_path=TARGET_PATH)
    ui_filters = DashboardFilters(model=dw_model)
    dashboard_app = SalesDashboardView(model=dw_model, filters=ui_filters)
    dashboard_app.render_app_layout()
