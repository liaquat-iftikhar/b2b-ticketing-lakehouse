import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict

from src.jobs.common.lakehouse_path_resolver import LakehousePathResolver
from src.jobs.common.spark_session_builder import SparkSessionFactory


class BaseDataPipeline(ABC):
    """
    Abstract Base Class establishing the definitive ETL template lifecycle. Manages Spark context initialization,
    automatic path determination, and reliable compute resource teardowns.
    """

    def __init__(
            self,
            execution_date: str,
            layer: str,
            etl_name: str,
            custom_base_path: Optional[str] = None,
            extra_configs: Optional[Dict[str, str]] = None
    ) -> None:
        self.execution_date = execution_date
        self.layer = layer
        self.etl_name = etl_name
        self.custom_base_path = custom_base_path
        self.extra_configs = extra_configs or {}
        self.spark = None
        self.logger = logging.getLogger(__name__)

        # Automatically resolve storage boundaries using central path utility
        self.target_dir, self.checkpoint_dir = LakehousePathResolver(custom_base_path=self.custom_base_path).resolve(
            layer=self.layer, etl_name=self.etl_name,
        )

    @abstractmethod
    def _extract(self):
        """Extracts raw datasets from the preceding Medallion layer."""
        pass

    @abstractmethod
    def _transform(self, df):
        """Applies business transformation, cleansing, and quarantine filtering rules."""
        pass

    @abstractmethod
    def _load(self, df_clean, df_quarantine):
        """Commits pristine data records and outcasts to their target storage layers."""
        pass

    def execute(self) -> None:
        """
        The orchestrating template method handling the secure execution loop lifecycle.
        Guarantees session compilation via the factory pattern and post-run cleanup hooks.
        """
        app_name = f"{self.layer}-{self.etl_name}-{self.execution_date}"
        self.logger.info("Initializing structural ETL lifecycle loop for: '%s'", app_name)

        # 1. Initialize configuration context via the factory instance
        factory = SparkSessionFactory(
            app_name=app_name,
            target_dir=self.target_dir,
            extra_configs=self.extra_configs
        )

        try:
            # 2. Extract active compute session
            self.spark = factory.get_or_create()

            # 3. Stream sequential lifecycle pipeline stages
            raw_data = self._extract()
            clean_data, quarantined_data = self._transform(raw_data)
            self._load(clean_data, quarantined_data)

            self.logger.info("Successfully completed extraction workflow loop for execution partition: %s",
                        self.execution_date)

        except Exception as exc:
            self.logger.critical("Fatal failure caught during master pipeline run sequence: %s", exc, exc_info=True)
            raise RuntimeError(f"Pipeline execution broke inside tracking envelope for: {app_name}") from exc

        finally:
            if self.spark:
                self.logger.info("Closing active resource hooks. Terminating SparkSession container context.")
                self.spark.stop()