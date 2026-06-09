import argparse
import logging
import os
from typing import Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.app_config import DatabaseConfig
from src.jobs.common.constants import Constants
from src.jobs.core.base_data_pipeline import BaseDataPipeline
from src.jobs.core.bronze.db_config_manager import (
    DBConnectionManager,
    JDBCPartitionConfig,
)


class JDBCDatabaseExtractor(BaseDataPipeline):
    """
    Reads a source database table via JDBC and writes it to the Bronze Delta layer.

    Supports optional parallel reads by accepting a JDBCPartitionConfig. Without
    one, Spark uses a single partition (suitable for small or reference tables).
    """

    def __init__(
        self,
        db_table: str,
        execution_date: str,
        connection_manager: DBConnectionManager,
        partition_config: Optional[JDBCPartitionConfig] = None,
        lakehouse_base_path: Optional[str] = None,
    ) -> None:
        self.db_table = db_table
        self.partition_config = partition_config
        self.jdbc_url = connection_manager.jdbc_url
        self.db_properties = connection_manager.db_properties
        self.logger = logging.getLogger(__name__)

        super().__init__(
            execution_date=execution_date,
            layer=Constants.LAYERS.BRONZE,
            etl_name=f"{db_table.lower()}_etl",
            lakehouse_base_path=lakehouse_base_path,
        )

    def _extract(self) -> DataFrame:
        self.logger.info("Reading table '%s' via JDBC.", self.db_table)

        reader = self.spark.read.format("jdbc").options(
            url=self.jdbc_url, dbtable=self.db_table, **self.db_properties
        )

        if self.partition_config:
            self.logger.info(
                "Applying partition config: column=%s, bounds=[%s, %s], partitions=%s",
                self.partition_config.column,
                self.partition_config.lower_bound,
                self.partition_config.upper_bound,
                self.partition_config.num_partitions,
            )
            reader = (
                reader.option("partitionColumn", self.partition_config.column)
                .option("lowerBound", self.partition_config.lower_bound)
                .option("upperBound", self.partition_config.upper_bound)
                .option("numPartitions", self.partition_config.num_partitions)
            )

        return reader.load()

    def _transform(self, df: DataFrame) -> Tuple[DataFrame, Optional[DataFrame]]:
        """Add Bronze lineage columns: execution date, ingest timestamp, and source table name."""
        self.logger.info("Adding lineage columns to '%s' extract.", self.db_table)

        enriched_df = (
            df.withColumn(
                "lh_bronze_execution_date", F.to_date(F.lit(self.execution_date))
            )
            .withColumn("lh_bronze_inserted_timestamp", F.current_timestamp())
            .withColumn("lh_bronze_source_database_table", F.lit(self.db_table))
        )

        return enriched_df, None

    def _load(
        self, df_clean: DataFrame, df_quarantine: Optional[DataFrame] = None
    ) -> None:
        target_dir = self.lakehouse_paths.target_dir
        self.logger.info("Writing Bronze Delta table to: %s", target_dir)

        (
            df_clean.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(target_dir)
        )
        self.logger.info("Bronze ingestion complete for table '%s'.", self.db_table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze JDBC ingestion job")
    parser.add_argument("--table", required=True, help="Source database table name.")
    parser.add_argument("--execution_date", required=True, help="Logical date (YYYY-MM-DD).")
    parser.add_argument(
        "--lakehouse_base_path",
        required=True,
        help="Lakehouse root — local path or s3a:// URI.",
    )
    parser.add_argument("--db_host", required=True)
    parser.add_argument("--db_user", required=True)
    parser.add_argument("--db_password", required=True)
    parser.add_argument("--db_port", required=True)
    parser.add_argument(
        "--db_driver", default="org.postgresql.Driver", help="JDBC driver class."
    )
    args = parser.parse_args()

    os.environ["DB_HOST"] = args.db_host
    os.environ["DB_PORT"] = args.db_port
    os.environ["DB_USER"] = args.db_user
    os.environ["DB_PASSWORD"] = args.db_password
    os.environ["DB_DRIVER"] = args.db_driver

    db_config = DatabaseConfig(
        host=args.db_host,
        port=int(args.db_port),
        user=args.db_user,
        password=args.db_password,
        name="event_platform",
        driver=args.db_driver,
    )

    conn_manager = DBConnectionManager(db_config=db_config)

    # Uncomment and tune for large tables that benefit from parallel reads:
    # tuning_strategy = JDBCPartitionConfig(
    #     column="id",
    #     lower_bound="1",
    #     upper_bound="1000000",
    #     num_partitions="8",
    # )
    tuning_strategy = None

    extractor = JDBCDatabaseExtractor(
        db_table=args.table,
        execution_date=args.execution_date,
        connection_manager=conn_manager,
        partition_config=tuning_strategy,
        lakehouse_base_path=args.lakehouse_base_path,
    )
    extractor.execute()
