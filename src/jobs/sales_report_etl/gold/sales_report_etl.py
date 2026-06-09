import argparse
import logging
import os
from dataclasses import dataclass
from typing import Optional, Tuple
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.jobs.common.constants import Constants
from src.jobs.core.base_data_pipeline import BaseDataPipeline
from src.jobs.common.lakehouse_path_resolver import LakehousePathResolver


@dataclass(frozen=True)
class UnifiedSalesSources:
    sale: DataFrame
    reseller_sale: DataFrame
    event: DataFrame
    commission_agreement: DataFrame
    ticket: DataFrame
    customer: DataFrame
    reseller: DataFrame


class SalesReportETL(BaseDataPipeline):
    """
    Gold ETL that merges platform sales and reseller CSV sales into one unified fact table.

    Joins against Silver dimension tables (event, ticket, customer, reseller,
    commission_agreement) to produce a denormalized analytical dataset.
    """

    def __init__(
        self,
        execution_date: str,
        lakehouse_base_path: Optional[str] = None,
        etl_name: str = "sales_report_etl",
    ) -> None:
        self.execution_date = execution_date
        self.logger = logging.getLogger(__name__)

        super().__init__(
            execution_date=execution_date,
            layer=Constants.LAYERS.GOLD,
            etl_name=etl_name,
            lakehouse_base_path=lakehouse_base_path,
        )

        resolver = LakehousePathResolver(lakehouse_base_path=lakehouse_base_path)
        self.silver_sales_dir = resolver.resolve(
            layer=Constants.LAYERS.SILVER, etl_name="sale_etl"
        ).target_dir
        self.silver_event_dir = resolver.resolve(
            layer=Constants.LAYERS.SILVER, etl_name="event_etl"
        ).target_dir
        self.silver_ticket_dir = resolver.resolve(
            layer=Constants.LAYERS.SILVER, etl_name="ticket_etl"
        ).target_dir
        self.silver_customer_dir = resolver.resolve(
            layer=Constants.LAYERS.SILVER, etl_name="customer_etl"
        ).target_dir
        self.silver_reseller_dir = resolver.resolve(
            layer=Constants.LAYERS.SILVER, etl_name="reseller_etl"
        ).target_dir
        self.silver_reseller_sale_dir = resolver.resolve(
            layer=Constants.LAYERS.SILVER, etl_name="reseller_sale_etl"
        ).target_dir
        self.silver_commission_agreement_dir = resolver.resolve(
            layer=Constants.LAYERS.SILVER, etl_name="commission_agreement_etl"
        ).target_dir

    def _extract(self) -> UnifiedSalesSources:
        """
        Load today's incremental sales partitions and full dimension snapshots from Silver.

        Sale and reseller_sale tables are filtered to the execution_date partition.
        Lookup tables (event, ticket, customer, reseller, commission_agreement) are
        loaded in full because they are small enough to broadcast.
        """
        self.logger.info("Extracting Silver data for partition: %s", self.execution_date)

        sale_df = (
            self.spark.read.format("delta")
            .load(self.silver_sales_dir)
            .filter(
                F.col("lh_silver_execution_date")
                == F.to_date(F.lit(self.execution_date))
            )
        )

        reseller_sale_df = (
            self.spark.read.format("delta")
            .load(self.silver_reseller_sale_dir)
            .filter(
                F.col("lh_silver_execution_date")
                == F.to_date(F.lit(self.execution_date))
            )
        )

        event_df = self.spark.read.format("delta").load(self.silver_event_dir)
        commission_agreement_df = self.spark.read.format("delta").load(
            self.silver_commission_agreement_dir
        )
        ticket_df = self.spark.read.format("delta").load(self.silver_ticket_dir)
        customer_df = self.spark.read.format("delta").load(self.silver_customer_dir)
        reseller_df = self.spark.read.format("delta").load(self.silver_reseller_dir)

        return UnifiedSalesSources(
            sale=sale_df,
            reseller_sale=reseller_sale_df,
            event=event_df,
            commission_agreement=commission_agreement_df,
            ticket=ticket_df,
            customer=customer_df,
            reseller=reseller_df,
        )

    def _transform(
        self, sources: UnifiedSalesSources
    ) -> Tuple[DataFrame, Optional[DataFrame]]:
        self.logger.info("Joining Silver dimensions and unioning platform + reseller sales.")

        pruned_events = sources.event.select(
            "event_id", "organizer_id", "event_name", "type", "region"
        )
        pruned_tickets = sources.ticket.select("ticket_id", "event_id", "category")
        pruned_customers = sources.customer.select(
            "customer_id", "first_name", "last_name"
        )
        pruned_resellers = sources.reseller.select("reseller_id", "country")
        pruned_comm = sources.commission_agreement.select(
            "agreement_id", "organizer_id", "reseller_id", "commission_rate"
        )

        # Normalize Event Lookup Strings
        reseller_sale_cleaned_df = sources.reseller_sale.withColumn(
            "clean_event_name", F.trim(F.lower(F.col("event_name")))
        )
        event_cleaned_df = pruned_events.withColumn(
            "clean_event_name", F.trim(F.lower(F.col("event_name")))
        )

        enriched_reseller_sale_df = (
            reseller_sale_cleaned_df.alias("res_sale")
            .join(
                F.broadcast(event_cleaned_df).alias("event"), "clean_event_name", "left"
            )
            .join(
                F.broadcast(pruned_comm).alias("commission"),
                (F.col("event.organizer_id") == F.col("commission.organizer_id"))
                & (
                    F.col("res_sale.reseller_id").eqNullSafe(
                        F.col("commission.reseller_id")
                    )
                ),
                "left",
            )
            .select(
                F.col("res_sale.transaction_id").cast(StringType()).alias("sale_id"),
                F.col("res_sale.clean_event_name").alias("event_name"),
                F.col("event.type").alias("event_type"),
                F.col("event.region").alias("event_region"),
                F.lit(None).cast(StringType()).alias("ticket_category"),
                F.col("res_sale.tickets_count"),
                F.col("res_sale.net_revenue").alias("total_amount"),
                F.coalesce(
                    (
                        (
                            F.col("res_sale.net_revenue")
                            * F.col("commission.commission_rate")
                        )
                        / 100
                    ),
                    F.lit(0.00),
                )
                .cast("decimal(10,2)")
                .alias("commission_amount"),
                F.coalesce(F.col("commission.commission_rate"), F.lit(0.00))
                .cast("decimal(5,2)")
                .alias("commission_rate_applied"),
                F.col("res_sale.sales_channel").alias("sales_channel"),
                F.col("res_sale.customer_first_name"),
                F.col("res_sale.customer_last_name"),
                F.col("res_sale.reseller_id"),
                F.col("res_sale.sale_date"),
                F.col("res_sale.office_location").alias("reseller_location"),
                F.lit("csv_import").alias("source"),
            )
        )

        standardized_platform_sale_df = (
            sources.sale.alias("sale")
            .join(F.broadcast(pruned_tickets).alias("ticket"), "ticket_id", "left")
            .join(
                F.broadcast(event_cleaned_df).alias("event"),
                (F.col("ticket.event_id") == F.col("event.event_id")),
                "left",
            )
            .join(F.broadcast(pruned_comm).alias("commission"), "agreement_id", "left")
            .join(
                F.broadcast(pruned_customers).alias("customer"), "customer_id", "left"
            )
            .join(
                F.broadcast(pruned_resellers).alias("reseller"), "reseller_id", "left"
            )
        ).select(
            F.col("sale.sale_id"),
            F.col("event.event_name").alias("event_name"),
            F.col("event.type").alias("event_type"),
            F.col("event.region").alias("event_region"),
            F.col("ticket.category").alias("ticket_category"),
            F.col("sale.quantity").alias("tickets_count"),
            F.col("sale.total_amount"),
            F.col("sale.commission_amount"),
            F.col("commission.commission_rate").alias("commission_rate_applied"),
            F.col("sale.channel").alias("sales_channel"),
            F.col("customer.first_name").alias("customer_first_name"),
            F.col("customer.last_name").alias("customer_last_name"),
            F.col("sale.reseller_id"),
            F.col("sale.sale_date"),
            F.col("reseller.country").alias("reseller_location"),
            F.lit("platform").alias("source"),
        )

        df_unified_sales = standardized_platform_sale_df.unionByName(
            enriched_reseller_sale_df
        )
        df_final_clean = (
            df_unified_sales
            .dropDuplicates(["sale_id", "reseller_id"])
            .withColumn("lh_gold_execution_date", F.to_date(F.lit(self.execution_date)))
            .withColumn("lh_gold_inserted_timestamp", F.current_timestamp())
        )

        return df_final_clean, None

    def _load(
        self, df_clean: DataFrame, df_quarantine: Optional[DataFrame] = None
    ) -> None:
        delta_log_path = os.path.join(self.lakehouse_paths.target_dir, "_delta_log")
        table_exists = DeltaTable.isDeltaTable(
            self.spark, self.lakehouse_paths.target_dir
        ) and os.path.exists(delta_log_path)

        if not table_exists:
            self.logger.info(
                "Gold table not found at %s — writing initial snapshot.",
                self.lakehouse_paths.target_dir,
            )
            (
                df_clean.write.format("delta")
                .mode("overwrite")
                .save(self.lakehouse_paths.target_dir)
            )
        else:
            self.logger.info(
                "Gold table exists — running MERGE on (sale_id, reseller_id)."
            )
            target_table = DeltaTable.forPath(
                self.spark, self.lakehouse_paths.target_dir
            )
            merge_condition = (
                "target.sale_id = updates.sale_id"
                " AND target.reseller_id IS NOT DISTINCT FROM updates.reseller_id"
            )
            (
                target_table.alias("target")
                .merge(
                    source=df_clean.alias("updates"),
                    condition=merge_condition,
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        self.logger.info("Gold Delta table load complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold sales report ETL job")
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--lakehouse_base_path", default=None)
    args = parser.parse_args()

    transformer = SalesReportETL(
        execution_date=args.execution_date, lakehouse_base_path=args.lakehouse_base_path
    )
    transformer.execute()
