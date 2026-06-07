import logging
import dataclasses
from abc import ABC, abstractmethod
from typing import Optional, Dict, Tuple
from pyspark.sql import DataFrame

from src.config.app_config import AppConfig, SparkConfig
from src.config.config_loader import ConfigLoader
from src.jobs.common.lakehouse_path_resolver import LakehousePathResolver
from src.jobs.common.spark_session_builder import SparkSessionFactory


class BaseDataPipeline(ABC):
    """
    Abstract Base Class establishing the definitive ETL template lifecycle.
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
        ).resolve(
            layer=self.layer,
            etl_name=self.etl_name,
        )

    @abstractmethod
    def _extract(self) -> DataFrame:
        """Extracts raw datasets from the preceding Medallion layer."""
        pass

    @abstractmethod
    def _transform(self, df: DataFrame) -> Tuple[DataFrame, Optional[DataFrame]]:
        """Applies business transformation, cleansing, and quarantine filtering rules."""
        pass

    @abstractmethod
    def _load(self, df_clean: DataFrame, df_quarantine: Optional[DataFrame]) -> None:
        """Commits pristine data records and outcasts to their target storage layers."""
        pass

    def execute(self) -> None:
        """
        Orchestrating template method that handles the secure execution lifecycle:
        builds the SparkSession from config, runs ETL stages, and guarantees teardown.
        """
        app_name = f"{self.layer}-{self.etl_name}-{self.execution_date}"
        self.logger.info("Initializing ETL lifecycle for: '%s'", app_name)

        spark_cfg: SparkConfig = dataclasses.replace(
            self._app_config.spark,
            app_name=app_name,
            extra_configs={**self._app_config.spark.extra_configs, **self.extra_configs},
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

            self.logger.info("Pipeline completed for execution partition: %s", self.execution_date)

        except Exception as exc:
            self.logger.critical("Fatal failure in pipeline '%s': %s", app_name, exc, exc_info=True)
            raise RuntimeError(f"Pipeline execution failed for: {app_name}") from exc

        finally:
            if self.spark:
                self.logger.info("Stopping SparkSession.")
                self.spark.stop()
