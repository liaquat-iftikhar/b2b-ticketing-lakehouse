import argparse
import logging
import os
from typing import Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.app_config import DatabaseConfig
from src.jobs.common.constants import Constants
from src.jobs.core.base_data_pipeline import BaseDataPipeline
from src.jobs.core.bronze.db_config_manager import DBConnectionManager, JDBCPartitionConfig


class JDBCDatabaseExtractor(BaseDataPipeline):
    """
    Handles high-throughput, parallelized extraction of relational database tables via JDBC directly into the raw
    Bronze Delta tier of the Lakehouse. Accepts dynamic execution credentials supplied by upstream orchestrators.
    """

    def __init__(
            self,
            db_table: str,
            execution_date: str,
            connection_manager: DBConnectionManager,
            partition_config: Optional[JDBCPartitionConfig] = None,
            lakehouse_base_path: Optional[str] = None
    ) -> None:

        self.db_table = db_table
        self.partition_config = partition_config

        # Unpack connection details directly from the manager object
        self.jdbc_url = connection_manager.jdbc_url
        self.db_properties = connection_manager.db_properties

        self.logger = logging.getLogger(__name__)

        super().__init__(
            execution_date=execution_date,
            layer=Constants.LAYERS.BRONZE,
            etl_name=f"{db_table.lower()}_etl",
            lakehouse_base_path=lakehouse_base_path
        )

    def _extract(self) -> DataFrame:
        """Establishes parallelized JDBC partition read pathways to dump source tables."""
        self.logger.info("Initializing JDBC reader connection pool for table: %s", self.db_table)

        reader = self.spark.read.format("jdbc").options(
            url=self.jdbc_url,
            dbtable=self.db_table,
            **self.db_properties
        )

        if self.partition_config:
            self.logger.info("Injecting dynamic optimization partition configurations into the JDBC read graph.")
            reader = (
                reader
                .option("partitionColumn", self.partition_config.column)
                .option("lowerBound", self.partition_config.lower_bound)
                .option("upperBound", self.partition_config.upper_bound)
                .option("numPartitions", self.partition_config.num_partitions)
            )

        return reader.load()

    def _transform(self, df: DataFrame) -> Tuple[DataFrame, Optional[DataFrame]]:
        """Applies infrastructure data lineage stamps to incoming source records."""
        self.logger.info("Stamping database payloads with active ingestion lineage properties...")

        enriched_df = (
            df
            .withColumn("lh_bronze_execution_date", F.to_date(F.lit(self.execution_date)))
            .withColumn("lh_bronze_inserted_timestamp", F.current_timestamp())
            .withColumn("lh_bronze_source_database_table", F.lit(self.db_table))
        )

        return enriched_df, None

    def _load(self, df_clean: DataFrame, df_quarantine: Optional[DataFrame] = None) -> None:
        """Appends raw operational system database extracts directly into a Bronze Delta table."""
        target_dir = self.lakehouse_paths.target_dir
        self.logger.info("Writing transactional database snapshot to Bronze Delta location: %s", target_dir)

        (
            df_clean.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(target_dir)
        )
        self.logger.info("Database extraction ingestion sequence completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Relational Database Medallion Bronze Ingestion Job")
    parser.add_argument("--table", required=True, help="Target relational database source table name string.")
    parser.add_argument("--execution_date", required=True, help="Logical date execution block (YYYY-MM-DD)")
    parser.add_argument("--lakehouse_base_path", required=True, help="Base directory for lakehouse. Supports local system paths or s3a:// bucket URIs.")

    # Explicit connection parameters exposed to CLI for ad-hoc manual testing runs
    parser.add_argument("--db_host", required=True, help="The host URI string.")
    parser.add_argument("--db_user", required=True, help="Authorized username credential identifier.")
    parser.add_argument("--db_password", required=True, help="Authorized execution security secret token.")
    parser.add_argument("--db_port", required=True, help="Port to connect to database.")
    parser.add_argument("--db_driver", default="org.postgresql.Driver",
                        help="Java target execution class connector driver.")
    args = parser.parse_args()

    os.environ["DB_HOST"] = args.db_host
    os.environ["DB_PORT"] = args.db_port
    os.environ["DB_USER"] = args.db_user
    os.environ["DB_PASSWORD"] = args.db_password
    os.environ["DB_DRIVER"] = args.db_driver
    os.environ["DB_DRIVER"] = args.db_driver

    db_config = DatabaseConfig(
        host=args.db_host,
        port=int(args.db_port),
        user=args.db_user,
        password=args.db_password,
        name="event_platform",  # Target source transaction database name
        driver="org.postgresql.Driver"
    )

    conn_manager = DBConnectionManager(db_config=db_config)

    # tuning_strategy = JDBCPartitionConfig(
    #     column="id",
    #     lower_bound="1",
    #     upper_bound="1000000",
    #     num_partitions="8"
    # )
    tuning_strategy = None
    extractor = JDBCDatabaseExtractor(
        db_table=args.table,
        execution_date=args.execution_date,
        connection_manager=conn_manager,
        partition_config=tuning_strategy,
        lakehouse_base_path=args.lakehouse_base_path
    )
    extractor.execute()
