import argparse
import logging
from typing import Optional, Tuple, Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.jobs.common.constants import Constants
from src.jobs.core.base_data_pipeline import BaseDataPipeline


class JDBCDatabaseExtractor(BaseDataPipeline):
    """
    Handles high-throughput, parallelized extraction of relational database tables via JDBC directly into the raw
    Bronze Delta tier of the Lakehouse. Accepts dynamic execution credentials supplied by upstream orchestrators.
    """

    def __init__(
            self,
            db_table: str,
            execution_date: str,
            jdbc_url: str,
            db_properties: Dict[str, str],
            partition_config: Optional[Dict] = None,
            target_base_path: Optional[str] = None
    ) -> None:

        self.db_table = db_table
        self.partition_config = partition_config or {}

        self.jdbc_url = jdbc_url
        self.db_properties = db_properties

        self.logger = logging.getLogger(__name__)

        super().__init__(
            execution_date=execution_date,
            layer=Constants.LAYERS.BRONZE,
            etl_name=f"db_ingest_{db_table.lower()}",
            custom_base_path=target_base_path
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
                .option("partitionColumn", self.partition_config["column"])
                .option("lowerBound", self.partition_config["lower_bound"])
                .option("upperBound", self.partition_config["upper_bound"])
                .option("numPartitions", self.partition_config["num_partitions"])
            )

        return reader.load()

    def _transform(self, df: DataFrame) -> Tuple[DataFrame, Optional[DataFrame]]:
        """Applies infrastructure data lineage stamps to incoming source records."""
        self.logger.info("Stamping database payloads with active ingestion lineage properties...")

        enriched_df = (
            df
            .withColumn("_dl_bronze_inserted_at", F.current_timestamp())
            .withColumn("_dl_bronze_source_database_table", F.lit(self.db_table))
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

    # ADDED: Explicit connection parameters exposed to CLI
    parser.add_argument("--jdbc_url", required=True, help="The complete JDBC target destination host URI string.")
    parser.add_argument("--db_user", required=True,
                        help="Authorized connection execution username credential identifier.")
    parser.add_argument("--db_password", required=True, help="Authorized connection execution security secret token.")
    parser.add_argument("--db_driver", default="org.postgresql.Driver",
                        help="Java target execution class connector driver.")
    args = parser.parse_args()

    tuning_matrix = {
        "column": "id",
        "lower_bound": "1",
        "upper_bound": "1000000",
        "num_partitions": "8"
    }

    # Restructure arguments cleanly back into the dictionary expected by PySpark
    properties_matrix = {
        "user": args.db_user,
        "password": args.db_password,
        "driver": args.db_driver
    }

    extractor = JDBCDatabaseExtractor(
        db_table=args.table,
        execution_date=args.execution_date,
        jdbc_url=args.jdbc_url,
        db_properties=properties_matrix,
        partition_config=tuning_matrix
    )
    extractor.execute()
