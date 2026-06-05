import os
import argparse
import logging
from pathlib import Path
from typing import Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from src.jobs.common.constants import Constants
from src.jobs.core.base_data_pipeline import BaseDataPipeline


class IncrementalResellerDataExtractor(BaseDataPipeline):
    """
    Handles the stateful, incremental ingestion of raw file landing zones
    into the immutable Bronze storage tier of the Lakehouse.
    """

    # Explicit Inbound Data Contract Schema
    DATA_CONTRACT_SCHEMA = StructType([
        StructField("Transaction ID", StringType(), True),
        StructField("Event name", StringType(), True),
        StructField("Number of purchased tickets", StringType(), True),
        StructField("Total amount", StringType(), True),
        StructField("Sales channel", StringType(), True),
        StructField("Customer first name", StringType(), True),
        StructField("Customer last name", StringType(), True),
        StructField("Office location", StringType(), True),
        StructField("Created Date", StringType(), True)
    ])

    def __init__(self, source_path: str, execution_date: str, target_base_path: Optional[str] = None) -> None:
        """
        Initializes the concrete pipeline extractor and bubbles configurations up
        to the BaseDataPipeline control layer.
        """
        self.source_path = source_path
        self.logger = logging.getLogger(__name__)

        super().__init__(
            execution_date=execution_date,
            layer=Constants.LAYERS.BRONZE,
            etl_name="reseller_etl",
            custom_base_path=target_base_path
        )

    def _extract(self) -> DataFrame:

        if not self.source_path.startswith("s3a://") and not os.path.exists(self.source_path):
            raise FileNotFoundError(f"Configured localized landing zone path '{self.source_path}' does not exist.")

        self.logger.info("Binding Structured Stream reader pipeline to source directory: %s", self.source_path)
        return self.spark.readStream \
            .format("csv") \
            .option("header", "true") \
            .schema(self.DATA_CONTRACT_SCHEMA) \
            .load(self.source_path)

    def _transform(self, df: DataFrame) -> Tuple[DataFrame, Optional[DataFrame]]:
        """Applies ingestion enrichment and file metadata lineage tracking metrics."""
        self.logger.info("Enriching streaming data payloads with active lineage tracing metadata...")

        # Append origin file metadata to track structural provenance
        enriched_stream_df = df.withColumn("input_file_name", F.input_file_name())

        # Bronze tier maintains raw structural duplicates; no separate quarantine split needed here
        return enriched_stream_df, None

    def _load(self, df_clean: DataFrame, df_quarantine: Optional[DataFrame] = None) -> None:
        """Executes the stateful stream query writer and awaits file accumulation termination."""
        self.logger.info("Streaming incremental data payloads to Bronze destination: %s",
                         self.lakehouse_paths.target_dir)
        self.logger.info("Writing streaming state checkpoints to: %s", self.lakehouse_paths.checkpoint_dir)

        query = (
            df_clean
            .writeStream
            .format("parquet")
            .outputMode("append")
            .option("checkpointLocation", self.lakehouse_paths.checkpoint_dir)
            .trigger(availableNow=True)
            .start(self.lakehouse_paths.target_dir)
        )

        # Block the execution thread until the dynamic micro-batch run process catches up
        query.awaitTermination()
        self.logger.info("Successfully completed file extraction migration from inbound to bronze.")


if __name__ == "__main__":
    project_root_dir = Path(__file__).resolve().parents[4]
    default_inbound = str(project_root_dir / "data" / "inbound")

    parser = argparse.ArgumentParser(description="Configurable Incremental File Consumer Ingestion Tool")
    parser.add_argument(
        "--source_path",
        default=default_inbound,
        help="Path where incoming source raw files are landed."
    )
    parser.add_argument(
        "--execution_date",
        default="2026-06-05",  # Defaults to active execution timeline anchor
        help="Processing logic state context day partition (Format: YYYY-MM-DD)."
    )
    parser.add_argument(
        "--target_base_path",
        default=None,
        help="Target base directory. Supports local system paths or s3a:// bucket URIs."
    )
    args = parser.parse_args()

    extractor = IncrementalResellerDataExtractor(
        source_path=args.source_path,
        execution_date=args.execution_date,
        target_base_path=args.target_base_path
    )

    extractor.execute()
