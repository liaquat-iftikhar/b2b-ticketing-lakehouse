import dataclasses
import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple

from pyspark.sql import DataFrame

from src.config.app_config import AppConfig, SparkConfig
from src.config.config_loader import ConfigLoader
from src.jobs.common.lakehouse_path_resolver import LakehousePathResolver
from src.jobs.common.spark_session_builder import SparkSessionFactory


class BaseDataPipeline(ABC):
    """
    Template base class for medallion ETL jobs.

    Subclasses implement _extract, _transform, and _load. The execute() method handles SparkSession lifecycle and
    guarantees the session is stopped even when a stage raises an exception.
    """

    def __init__(
        self,
        execution_date: str,
        layer: str,
        etl_name: str,
        lakehouse_base_path: Optional[str] = None,
        extra_configs: Optional[Dict[str, str]] = None,
    ) -> None:
        self.execution_date = execution_date
        self.layer = layer
        self.etl_name = etl_name
        self.extra_configs = extra_configs or {}
        self.spark = None
        self.logger = logging.getLogger(self.__class__.__name__)

        self._app_config: AppConfig = ConfigLoader.load()

        resolved_base = lakehouse_base_path or self._app_config.storage.base_path
        self.lakehouse_paths = LakehousePathResolver(
            lakehouse_base_path=resolved_base
        ).resolve(layer=self.layer, etl_name=self.etl_name)

    @abstractmethod
    def _extract(self) -> DataFrame:
        """Read data from the upstream layer."""

    @abstractmethod
    def _transform(self, df: DataFrame) -> Tuple[DataFrame, Optional[DataFrame]]:
        """
        Apply transformations and return (clean_df, quarantine_df).

        Return None for quarantine_df if the pipeline has no quarantine path.
        """

    @abstractmethod
    def _load(self, df_clean: DataFrame, df_quarantine: Optional[DataFrame]) -> None:
        """Write clean and quarantined data to their target locations."""

    def execute(self) -> None:
        """Run the ETL pipeline: build SparkSession, call stages, stop session."""
        app_name = f"{self.layer}-{self.etl_name}-{self.execution_date}"
        self.logger.info("Starting pipeline: %s", app_name)

        spark_cfg: SparkConfig = dataclasses.replace(
            self._app_config.spark,
            app_name=app_name,
            extra_configs={
                **self._app_config.spark.extra_configs,
                **self.extra_configs,
            },
        )

        factory = SparkSessionFactory(
            spark_config=spark_cfg,
            storage_config=self._app_config.storage,
        )

        try:
            self.spark = factory.get_or_create()

            raw_data = self._extract()
            clean_data, quarantined_data = self._transform(raw_data)
            self._load(clean_data, quarantined_data)

            self.logger.info("Pipeline complete for partition: %s", self.execution_date)

        except Exception as exc:
            self.logger.critical(
                "Pipeline '%s' failed: %s", app_name, exc, exc_info=True
            )
            raise RuntimeError(f"Pipeline execution failed for: {app_name}") from exc

        finally:
            if self.spark:
                self.logger.info("Stopping SparkSession.")
                self.spark.stop()
