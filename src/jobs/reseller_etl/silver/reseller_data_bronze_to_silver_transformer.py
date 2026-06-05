import os
import argparse
import logging
from typing import Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from delta.tables import DeltaTable

from src.jobs.common.constants import Constants
from src.jobs.common.lakehouse_path_resolver import LakehousePathResolver
from src.jobs.core.base_data_pipeline import BaseDataPipeline


class BronzeToSilverTransformer(BaseDataPipeline):
    """
    Incremental batch pipeline to move Reseller data from Bronze Delta to Silver Delta.
    Handles schema mapping, data cleansing, validation filters, and idempotent deduplication.
    """

    def __init__(self, execution_date: str, etl_name: str = "reseller_etl",
                 custom_base_path: Optional[str] = None) -> None:
        super().__init__(
            execution_date=execution_date,
            layer=Constants.LAYERS.SILVER,
            etl_name=etl_name,
            custom_base_path=custom_base_path
        )

        # Resolve upstream Bronze Delta table coordinates
        self.bronze_paths = LakehousePathResolver(custom_base_path=custom_base_path).resolve(
            layer=Constants.LAYERS.BRONZE,
            etl_name=etl_name
        )

        self.logger = logging.getLogger(__name__)

    def _extract(self) -> DataFrame:
        """Reads raw datasets from the upstream Bronze Delta table matching the execution date."""
        bronze_src = self.bronze_paths.target_dir

        if not bronze_src.startswith("s3a://") and not os.path.exists(bronze_src):
            raise FileNotFoundError(f"Bronze Delta storage layer path does not exist: {bronze_src}")

        self.logger.info("Reading raw datasets from Bronze Delta table: %s", bronze_src)

        # CHANGED: Read format switched from parquet to delta
        df_bronze = self.spark.read.format("delta").load(bronze_src)

        df_slice = df_bronze.filter((F.col("Created_Date") == self.execution_date))
        return df_slice

    def _transform(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Apply schema, routes cast and business failures to a quarantine dataset, and returns sanitized rows.
        """
        self.logger.info("Transforming bronze data datasets...")

        # Standardize schema definitions across multi-channel structures
        df_conformed = (
            df
            .withColumn("transaction_id", F.col("transaction_id").cast(IntegerType()))
            .withColumn("event_name", F.col("Event_name"))
            .withColumn("tickets_count", F.col("Number_of_purchased_tickets").cast(IntegerType()))
            .withColumn("raw_amount", F.col("Total_amount").cast("string"))
            .withColumn("clean_amount_str", F.regexp_replace(F.col("raw_amount"), r"[\$,]", ""))
            .withColumn("net_revenue", F.col("clean_amount_str").cast(DoubleType()))
            .withColumn("sales_channel", F.col("Sales_channel"))
            .withColumn("office_location", F.col("Office_location"))
            .withColumn("customer_first_name", F.col("Customer_first_name"))
            .withColumn("customer_last_name", F.col("Customer_last_name"))
            .withColumn("log_date", F.col("Created_Date"))
        )

        # Isolate Type-Cast Failures
        is_cast_failure = (
                (F.col("Transaction_ID").isNotNull() & F.col("transaction_id").isNull()) |
                (F.col("Number_of_purchased_tickets").isNotNull() & F.col("tickets_count").isNull()) |
                (F.col("clean_amount_str").isNotNull() & F.col("net_revenue").isNull())
        )

        df_cast_failures = (
            df_conformed
            .filter(is_cast_failure)
            .withColumn("quarantine_reason", F.lit("Type casting mismatch vulnerability"))
        )

        df_cast_passed = df_conformed.filter(~is_cast_failure)

        # Isolate Business Boundary Rule Violations
        is_business_invalid = (
                (F.col("tickets_count") <= 0) |
                (F.col("net_revenue") < 0) |
                (F.col("transaction_id").isNull())
        )

        df_business_failures = (
            df_cast_passed
            .filter(is_business_invalid)
            .withColumn("quarantine_reason", F.lit("Business validation rule limit infraction"))
        )

        df_quarantine = (
            df_cast_failures
            .unionByName(df_business_failures, allowMissingColumns=True)
            .withColumn("quarantined_at", F.current_timestamp())
        )

        # Deduplicate Validated Clean Records
        df_clean_base = df_cast_passed.filter(~is_business_invalid)

        df_deduped = (
            df_clean_base
            .dropDuplicates(["transaction_id", "reseller_id"])
            .select(
                "transaction_id", "reseller_id", "event_name", "tickets_count", "net_revenue", "sales_channel", "office_location",
                "customer_first_name", "customer_last_name", "log_date"
            )
            .withColumn("silver_inserted_at", F.current_timestamp())
        )

        return df_deduped, df_quarantine

    def _load(self, df_clean: DataFrame, df_quarantine: DataFrame) -> None:
        """Commits pristine records to Silver Delta via MERGE and dumps anomalies to Quarantine Delta."""
        silver_dir = self.lakehouse_paths.target_dir
        quarantine_dir = self.lakehouse_paths.quarantine_dir

        # 1. Idempotent Target Load for Clean Data (MERGE INTO)
        if df_clean.count() > 0:
            if DeltaTable.isDeltaTable(self.spark, silver_dir):
                self.logger.info("Performing row-level Delta MERGE into Silver target: %s", silver_dir)
                silver_table = DeltaTable.forPath(self.spark, silver_dir)

                (
                    silver_table.alias("target")
                    .merge(df_clean.alias("source"), "target.transaction_id = source.transaction_id")
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                self.logger.info("Silver Delta table uninitialized. Creating fresh table structure at: %s", silver_dir)
                (
                    df_clean.write
                    .format("delta")
                    .mode("overwrite")
                    .partitionBy("log_date")
                    .save(silver_dir)
                )
        else:
            self.logger.warning("No clean records processed to load for date: %s", self.execution_date)

        # 2. Target Load for Quarantine Records
        if df_quarantine.count() > 0:
            self.logger.warning("Found %d malformed rows. Offloading to Quarantine Delta path: %s",
                           df_quarantine.count(), quarantine_dir)
            (
                df_quarantine.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(quarantine_dir)
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Medallion Bronze-to-Silver Incremental Transform Pipeline")
    parser.add_argument("--execution_date", required=True, help="Processing date slice token (Format: YYYY-MM-DD)")
    parser.add_argument("--etl_name", default="reseller_etl", help="Target identity scope for lakehouse data folders.")
    parser.add_argument("--target_base_path", default=None, help="Overriding base target directory path URI string.")
    args = parser.parse_args()

    transformer = BronzeToSilverTransformer(
        execution_date=args.execution_date,
        etl_name=args.etl_name,
        custom_base_path=args.target_base_path
    )

    transformer.execute()
