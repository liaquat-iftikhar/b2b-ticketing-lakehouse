import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession

from src.jobs.reseller_etl.bronze.incremental_reseller_data_extractor import IncrementalResellerDataExtractor


class TestIncrementalResellerDataExtractor(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        """Initialize a local, isolated SparkSession for unit test execution."""
        cls.spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("UnitTest-IncrementalResellerDataExtractor")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        """Shutdown the shared SparkSession instance."""
        if cls.spark:
            cls.spark.stop()

    def setUp(self) -> None:
        """Set up temporary working directories for file simulation."""
        self.test_dir = tempfile.mkdtemp()
        self.source_path = os.path.join(self.test_dir, "inbound")
        self.target_base_path = os.path.join(self.test_dir, "lakehouse")

        os.makedirs(self.source_path, exist_ok=True)

    def tearDown(self) -> None:
        """Clean up temporary workspaces after test scenarios run."""
        shutil.rmtree(self.test_dir, ignore_errors=True)

    @patch("src.jobs.core.base_data_pipeline.SparkSessionFactory")
    def test_init_resolves_correct_lakehouse_paths(self, mock_spark_factory) -> None:
        """Verify that initialization accurately maps Medallion boundaries for both custom and default paths."""

        # Scenario A: Verifying path resolution when an explicit custom target base path is provided
        extractor_custom = IncrementalResellerDataExtractor(
            source_path=self.source_path,
            execution_date="2026-06-05",
            target_base_path=self.target_base_path
        )

        self.assertEqual(extractor_custom.source_path, self.source_path)
        # It should end with 'lakehouse/bronze' because custom path overrides default layout tokens
        self.assertTrue(extractor_custom.lakehouse_paths.target_dir.endswith("lakehouse/bronze"))
        self.assertTrue(
            extractor_custom.lakehouse_paths.checkpoint_dir.endswith("lakehouse/_checkpoints/reseller_etl_to_bronze"))

        # Scenario B: Verifying path resolution when falling back to the internal default workspace path
        extractor_default = IncrementalResellerDataExtractor(
            source_path=self.source_path,
            execution_date="2026-06-05",
            target_base_path=None  # Triggers default project root path resolution logic
        )

        # It should end with 'reseller_etl/bronze' when using internal fallbacks
        self.assertTrue(extractor_default.lakehouse_paths.target_dir.endswith("reseller_etl/bronze"))

    @patch("src.jobs.core.base_data_pipeline.SparkSessionFactory")
    def test_extract_raises_file_not_found_for_missing_source(self, mock_spark_factory) -> None:
        """Verify that _extract raises FileNotFoundError if the local source path does not exist."""
        # Arrange
        non_existent_path = os.path.join(self.test_dir, "ghost_directory")
        extractor = IncrementalResellerDataExtractor(
            source_path=non_existent_path,
            execution_date="2026-06-05",
            target_base_path=self.target_base_path
        )
        extractor.spark = self.spark

        # Act & Assert
        with self.assertRaises(FileNotFoundError):
            extractor._extract()

    @patch("src.jobs.core.base_data_pipeline.SparkSessionFactory")
    def test_transform_appends_lineage_metadata(self, mock_spark_factory) -> None:
        """Verify that _transform enriches the incoming dataset with input_file_name metadata."""
        # Arrange
        extractor = IncrementalResellerDataExtractor(
            source_path=self.source_path,
            execution_date="2026-06-05",
            target_base_path=self.target_base_path
        )

        # Build mock source batch data matching schema
        test_data = [("TX123", "Concert", "2", "100.00", "Web", "John", "Doe", "Berlin", "2026-06-05")]
        input_df = self.spark.createDataFrame(test_data, schema=extractor.DATA_CONTRACT_SCHEMA)

        # Act
        clean_df, quarantine_df = extractor._transform(input_df)

        # Assert
        self.assertIn("input_file_name", clean_df.columns)
        self.assertIsNone(quarantine_df)

        # Verify schema preservation
        self.assertEqual(len(clean_df.columns), len(extractor.DATA_CONTRACT_SCHEMA.names) + 2)

    @patch("src.jobs.core.base_data_pipeline.SparkSessionFactory")
    def test_load_starts_and_terminates_streaming_query(self, mock_spark_factory) -> None:
        """Verify that _load configures, runs, and terminates a writeStream query."""
        # Arrange
        extractor = IncrementalResellerDataExtractor(
            source_path=self.source_path,
            execution_date="2026-06-05",
            target_base_path=self.target_base_path
        )

        # Mock structural streaming outputs
        mock_query = MagicMock()
        mock_writer = MagicMock()
        mock_df_clean = MagicMock()

        # Build execution mock chain: df.writeStream.format.outputMode.option.trigger.start
        mock_df_clean.writeStream = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.outputMode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.trigger.return_value = mock_writer
        mock_writer.start.return_value = mock_query

        # Act
        extractor._load(df_clean=mock_df_clean, df_quarantine=None)

        # Assert
        mock_writer.format.assert_called_once_with("delta")
        mock_writer.outputMode.assert_called_once_with("append")
        mock_writer.option.assert_called_once_with("checkpointLocation", extractor.lakehouse_paths.checkpoint_dir)
        mock_writer.start.assert_called_once_with(extractor.lakehouse_paths.target_dir)
        mock_query.awaitTermination.assert_called_once()


if __name__ == "__main__":
    unittest.main()
