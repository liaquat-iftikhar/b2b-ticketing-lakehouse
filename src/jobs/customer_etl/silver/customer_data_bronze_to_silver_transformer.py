import os
import argparse
import logging
from typing import Optional, Tuple
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, TimestampType

from src.jobs.common.constants import Constants
from src.jobs.core.base_data_pipeline import BaseDataPipeline
from src.jobs.common.lakehouse_path_resolver import LakehousePathResolver


class CustomerDataBronzeToSilverTransformer(BaseDataPipeline):
    """
    Enforces type conformance, removes duplicates on Bronze Customer data and performs idempotent upserts (MERGE) into
    the Silver tier.
    """

    def __init__(self, execution_date: str, lakehouse_base_path: Optional[str] = None,
                 etl_name: str = "customers_etl") -> None:
        self.execution_date = execution_date
        self.logger = logging.getLogger(__name__)

        super().__init__(
            execution_date=execution_date,
            layer=Constants.LAYERS.SILVER,
            etl_name=etl_name,
            lakehouse_base_path=lakehouse_base_path
        )

        resolver = LakehousePathResolver(lakehouse_base_path=lakehouse_base_path)
        self.bronze_source_dir = resolver.resolve(layer="bronze", etl_name=etl_name).target_dir

    def _extract(self) -> DataFrame:
        self.logger.info("Extracting batch partition data from Customer Bronze directory: %s", self.bronze_source_dir)
        return (
            self.spark.read
            .format("delta")
            .load(self.bronze_source_dir)
            .filter(F.col("lh_bronze_execution_date") == F.to_date(F.lit(self.execution_date)))
        )

    def _transform(self, df: DataFrame) -> Tuple[DataFrame, Optional[DataFrame]]:
        self.logger.info("Executing transactional normalization and schema casting optimization matrix...")

        # Enforce strict data types and map from raw source names
        conformed_df = (
            df
            .withColumn("customer_id", F.col("id").cast(IntegerType()))
            .withColumn("name", F.col("name").cast(StringType()))
            .withColumn("email", F.trim(F.col("email")).cast(StringType()))
            .withColumn("profile_created_timestamp", F.col("_created_at").cast(TimestampType()))
            .withColumn("profile_last_updated_timestamp", F.col("_updated_at").cast(TimestampType()))
            .drop("id", "_created_at", "_updated_at")
        )

        window_spec = Window.partitionBy("customer_id").orderBy(F.col("lh_bronze_inserted_timestamp").desc())

        deduplicated_df = (
            conformed_df
            .withColumn("row_num", F.row_number().over(window_spec))
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )

        final_silver_df = (
            deduplicated_df
            .withColumn("lh_silver_execution_date", F.to_date(F.lit(self.execution_date)))
            .withColumn("lh_silver_inserted_timestamp", F.current_timestamp())
            .select(
                "customer_id", "name", "email", "profile_created_timestamp", "profile_last_updated_timestamp",
                "lh_bronze_execution_date", "lh_bronze_inserted_timestamp", "lh_bronze_source_database_table",
                "lh_silver_execution_date", "lh_silver_inserted_timestamp"
            )
        )
        return final_silver_df, None

    def _load(self, df_clean: DataFrame, df_quarantine: Optional[DataFrame] = None) -> None:
        delta_log_path = os.path.join(self.lakehouse_paths.target_dir, "_delta_log")
        table_exists = DeltaTable.isDeltaTable(self.spark, self.lakehouse_paths.target_dir) and os.path.exists(delta_log_path)

        if not table_exists:
            self.logger.info("Target transaction log is missing. Bootstrapping raw Customer Delta table...")
            (
                df_clean.write
                .format("delta")
                .mode("overwrite")
                .save(self.lakehouse_paths.target_dir)
            )
        else:
            self.logger.info("Target Delta table log structure confirmed. Executing merge...")
            target_table = DeltaTable.forPath(self.spark, self.lakehouse_paths.target_dir)
            (
                target_table.alias("target")
                .merge(
                    source=df_clean.alias("updates"),
                    condition="target.customer_id = updates.customer_id"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        self.logger.info("Successfully completed loading conformed metrics into Silver Customer Delta layer.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lakehouse Customers Silver Layer Transformer Engine")
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--lakehouse_base_path", required=True)
    args = parser.parse_args()

    transformer = CustomerDataBronzeToSilverTransformer(
        execution_date=args.execution_date,
        lakehouse_base_path=args.lakehouse_base_path
    )
    transformer.execute()