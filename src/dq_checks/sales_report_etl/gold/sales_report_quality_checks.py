import argparse
import logging
import sys
from typing import Optional

import duckdb
import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd
from great_expectations.data_context import AbstractDataContext

from src.config.config_loader import ConfigLoader
from src.jobs.common.constants import Constants
from src.jobs.common.lakehouse_path_resolver import LakehousePathResolver


class GoldTableLoader:
    """Handles low-level data extraction and I/O from the Lakehouse storage layers."""

    def __init__(self, gold_path: str):
        self.gold_path = gold_path
        self._logger = logging.getLogger(self.__class__.__name__)

    def load_partition_by_date(self, execution_date: str) -> pd.DataFrame:

        self._logger.info("Connecting to Gold Delta table target: %s", self.gold_path)

        connection = duckdb.connect()
        try:
            connection.execute("INSTALL delta; LOAD delta;")
            query = f"""
                SELECT *
                FROM delta_scan('{self.gold_path}')
            """
            dataframe: pd.DataFrame = connection.execute(query).df()
        finally:
            connection.close()

        self._logger.info(
            "Extracted %d rows from Gold storage for partition date %s",
            len(dataframe),
            execution_date,
        )
        return dataframe


class SalesReportExpectationSuiteBuilder:
    """Declares, defines, and encapsulates corporate data quality schemas and constraints."""

    SUITE_NAME = "sales_report_gold_suite"

    REQUIRED_COLUMNS = [
        "sale_id",
        "event_name",
        "event_type",
        "event_region",
        "tickets_count",
        "total_amount",
        "commission_amount",
        "commission_rate_applied",
        "sales_channel",
        "customer_first_name",
        "customer_last_name",
        "reseller_id",
        "sale_date",
        "reseller_location",
        "source",
    ]

    COMPLETENESS_COLUMNS = [
        "sale_id",
        "sale_date",
        "total_amount",
        "tickets_count",
        "sales_channel",
        "source",
        "customer_first_name",
        "customer_last_name",
    ]

    VALID_SOURCES = ["platform", "csv_import"]

    def build_suite(self, context: AbstractDataContext) -> gx.ExpectationSuite:

        suite = context.suites.add(gx.ExpectationSuite(name=self.SUITE_NAME))

        # 1. Completeness Validations
        for column in self.COMPLETENESS_COLUMNS:
            suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=column))

        # 2. Structural Schema Structural Matching
        suite.add_expectation(
            gxe.ExpectTableColumnsToMatchSet(
                column_set=self.REQUIRED_COLUMNS,
                exact_match=False,
            )
        )

        # 3. Uniqueness Contraints (Composite Merge Keys)
        suite.add_expectation(
            gxe.ExpectCompoundColumnsToBeUnique(column_list=["sale_id", "reseller_id"])
        )

        # 4. Value Range Bounds Checking (Numeric Sanity)
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0)
        )
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeBetween(column="tickets_count", min_value=1)
        )
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeBetween(column="commission_amount", min_value=0)
        )
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeBetween(
                column="commission_rate_applied", min_value=0, max_value=100
            )
        )

        # 5. Categorical Domain Enforcement
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeInSet(
                column="source", value_set=self.VALID_SOURCES
            )
        )

        # 6. Freshness / Activity Verification
        suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=1))

        return suite


class DataQualityValidator:
    """Manages the lifecycle of Great Expectations validation runs and reporting workflows."""

    def __init__(self, suite_builder: SalesReportExpectationSuiteBuilder):
        self.suite_builder = suite_builder
        self._logger = logging.getLogger(self.__class__.__name__)

    def execute_quality_gate(
        self, dataframe: pd.DataFrame, execution_date: str
    ) -> bool:
        """Executes full quality evaluations against a provided DataFrame asset."""
        self._logger.info(
            "Initializing ephemeral Great Expectations execution driver..."
        )
        context = gx.get_context(mode="ephemeral")

        # Build expectation components via injected blueprint builder
        suite = self.suite_builder.build_suite(context)

        # Provision batch ingestion paths
        data_source = context.data_sources.add_pandas(name="gold_sales_source")
        data_asset = data_source.add_dataframe_asset(name="gold_sales_fact")
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            name="full_execution_partition"
        )

        validation_definition = context.validation_definitions.add(
            gx.ValidationDefinition(
                name=f"sales_report_quality_{execution_date}",
                data=batch_definition,
                suite=suite,
            )
        )

        self._logger.info("Evaluating expectation profile against payload matrix...")
        result = validation_definition.run(batch_parameters={"dataframe": dataframe})

        self._log_validation_summary(result)
        return result.success

    def _log_validation_summary(self, result) -> None:
        """Parses run analytics structures and routes structural trace alerts to logs."""
        stats = result.statistics
        self._logger.info(
            "Quality verification pass wrapped — Evaluated: %d | Passed: %d | Failed: %d | Outcome: %s",
            stats["evaluated_expectations"],
            stats["successful_expectations"],
            stats["unsuccessful_expectations"],
            "SUCCESS" if result.success else "FAILURE",
        )

        if not result.success:
            self._logger.error("=" * 80)
            self._logger.error("DATA QUALITY EXCEPTION DETECTED — BREACH DETAILS:")
            for evaluation in result.results:
                if not evaluation.success:
                    column = (
                        evaluation.expectation_config.column
                        if hasattr(evaluation.expectation_config, "column")
                        else "Global Table Context"
                    )
                    self._logger.error(
                        "  ✗ Rule Type: %s | Evaluation Focus: %s | Error Trace Metrics: %s",
                        evaluation.expectation_config.type,
                        column,
                        evaluation.result,
                    )
            self._logger.error("=" * 80)


class GoldQualityGateApp:
    """App controller orchestrating path resolution, component assembly, and execution tracking."""

    def __init__(self, execution_date: str, lakehouse_base_path: Optional[str] = None):
        self.execution_date = execution_date
        self.lakehouse_base_path = lakehouse_base_path
        self._logger = logging.getLogger(self.__class__.__name__)

    def _resolve_gold_storage_path(self) -> str:
        """Calculates absolute storage targets via configuration systems."""
        config = ConfigLoader.load()
        base_path = self.lakehouse_base_path or config.storage.base_path

        resolver = LakehousePathResolver(lakehouse_base_path=base_path)
        return resolver.resolve(
            layer=Constants.LAYERS.GOLD,
            etl_name="sales_report_etl",
        ).target_dir

    def run(self) -> None:
        """Drives runtime integration flows, exiting on errors with standard POSIX codes."""
        try:
            gold_path = self._resolve_gold_storage_path()

            # Instantiate decoupled structural components (Dependency Assembly)
            loader = GoldTableLoader(gold_path=gold_path)
            suite_builder = SalesReportExpectationSuiteBuilder()
            validator = DataQualityValidator(suite_builder=suite_builder)

            # Process execution pipelines
            dataframe = loader.load_partition_by_date(
                execution_date=self.execution_date
            )
            gate_passed = validator.execute_quality_gate(
                dataframe=dataframe, execution_date=self.execution_date
            )

            if not gate_passed:
                self._logger.error(
                    "Data quality gate FAILED for execution date %s. Blocked further ingestion cascades.",
                    self.execution_date,
                )
                sys.exit(1)

            self._logger.info(
                "Data quality gate PASSED for execution date %s.", self.execution_date
            )
            sys.exit(0)

        except Exception as error:
            self._logger.exception(
                "Fatal processing loop disruption caught at application context: %s",
                error,
            )
            sys.exit(2)


# ── Executable Command Line Bootstrap ──────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Great Expectations quality checks for the Gold sales report table."
    )
    parser.add_argument(
        "--execution_date",
        required=True,
        help="Logical execution date (YYYY-MM-DD) — used to filter the Gold partition.",
    )
    parser.add_argument(
        "--lakehouse_base_path",
        default=None,
        help="Base lakehouse path (local or s3a://). Defaults to config value.",
    )
    args = parser.parse_args()

    # Launch application runtime instance
    app = GoldQualityGateApp(
        execution_date=args.execution_date, lakehouse_base_path=args.lakehouse_base_path
    )
    app.run()
