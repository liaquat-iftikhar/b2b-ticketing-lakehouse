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
    """Reads rows from the Gold Delta table via DuckDB."""

    def __init__(self, gold_path: str):
        self.gold_path = gold_path
        self._logger = logging.getLogger(self.__class__.__name__)

    def load_partition_by_date(self, execution_date: str) -> pd.DataFrame:
        """Return all Gold rows for the given execution_date partition."""
        self._logger.info("Reading Gold Delta table at: %s", self.gold_path)

        connection = duckdb.connect()
        try:
            connection.execute("INSTALL delta; LOAD delta;")
            dataframe: pd.DataFrame = connection.execute(
                f"""
                SELECT *
                FROM delta_scan('{self.gold_path}')
                WHERE CAST(lh_gold_execution_date AS DATE) = CAST(? AS DATE)
                """,
                [execution_date],
            ).df()
        finally:
            connection.close()

        self._logger.info(
            "Loaded %d rows for partition %s.", len(dataframe), execution_date
        )
        return dataframe


class SalesReportExpectationSuiteBuilder:
    """Builds the Great Expectations suite for the Gold sales report table."""

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

        # 2. Schema — required columns must all be present
        suite.add_expectation(
            gxe.ExpectTableColumnsToMatchSet(
                column_set=self.REQUIRED_COLUMNS,
                exact_match=False,
            )
        )

        # 3. Uniqueness — (sale_id, reseller_id) is the composite merge key
        suite.add_expectation(
            gxe.ExpectCompoundColumnsToBeUnique(column_list=["sale_id", "reseller_id"])
        )

        # 4. Numeric range checks
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

        # 5. Categorical values
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeInSet(
                column="source", value_set=self.VALID_SOURCES
            )
        )

        # 6. Non-empty partition check
        suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=1))

        return suite


class DataQualityValidator:
    """Runs a Great Expectations validation suite against a DataFrame and logs the results."""

    def __init__(self, suite_builder: SalesReportExpectationSuiteBuilder):
        self.suite_builder = suite_builder
        self._logger = logging.getLogger(self.__class__.__name__)

    def execute_quality_gate(
        self, dataframe: pd.DataFrame, execution_date: str
    ) -> bool:
        """Validate dataframe against the expectation suite; return True if all checks pass."""
        self._logger.info("Running DQ checks for partition: %s", execution_date)
        context = gx.get_context(mode="ephemeral")

        suite = self.suite_builder.build_suite(context)

        data_source = context.data_sources.add_pandas(name="gold_sales_source")
        data_asset = data_source.add_dataframe_asset(name="gold_sales_fact")
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            name="full_partition"
        )

        validation_definition = context.validation_definitions.add(
            gx.ValidationDefinition(
                name=f"sales_report_quality_{execution_date}",
                data=batch_definition,
                suite=suite,
            )
        )

        result = validation_definition.run(batch_parameters={"dataframe": dataframe})
        self._log_validation_summary(result)
        return result.success

    def _log_validation_summary(self, result) -> None:
        stats = result.statistics
        self._logger.info(
            "DQ run complete — evaluated: %d | passed: %d | failed: %d | outcome: %s",
            stats["evaluated_expectations"],
            stats["successful_expectations"],
            stats["unsuccessful_expectations"],
            "PASS" if result.success else "FAIL",
        )

        if not result.success:
            self._logger.error("Failed expectations:")
            for evaluation in result.results:
                if not evaluation.success:
                    column = (
                        evaluation.expectation_config.column
                        if hasattr(evaluation.expectation_config, "column")
                        else "table-level"
                    )
                    self._logger.error(
                        "  FAIL  type=%s  column=%s  result=%s",
                        evaluation.expectation_config.type,
                        column,
                        evaluation.result,
                    )


class GoldQualityGateApp:
    """Entry point that wires up loader, suite builder, and validator for a given partition."""

    def __init__(self, execution_date: str, lakehouse_base_path: Optional[str] = None):
        self.execution_date = execution_date
        self.lakehouse_base_path = lakehouse_base_path
        self._logger = logging.getLogger(self.__class__.__name__)

    def _resolve_gold_storage_path(self) -> str:
        config = ConfigLoader.load()
        base_path = self.lakehouse_base_path or config.storage.base_path
        resolver = LakehousePathResolver(lakehouse_base_path=base_path)
        return resolver.resolve(
            layer=Constants.LAYERS.GOLD,
            etl_name="sales_report_etl",
        ).target_dir

    def run(self) -> None:
        """Load the partition, run DQ checks, and exit with 0 (pass), 1 (fail), or 2 (error)."""
        try:
            gold_path = self._resolve_gold_storage_path()

            loader = GoldTableLoader(gold_path=gold_path)
            suite_builder = SalesReportExpectationSuiteBuilder()
            validator = DataQualityValidator(suite_builder=suite_builder)

            dataframe = loader.load_partition_by_date(execution_date=self.execution_date)
            gate_passed = validator.execute_quality_gate(
                dataframe=dataframe, execution_date=self.execution_date
            )

            if not gate_passed:
                self._logger.error(
                    "DQ gate FAILED for %s.", self.execution_date
                )
                sys.exit(1)

            self._logger.info("DQ gate PASSED for %s.", self.execution_date)
            sys.exit(0)

        except Exception as error:
            self._logger.exception("Unexpected error in DQ gate: %s", error)
            sys.exit(2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run Great Expectations checks on the Gold sales report partition."
    )
    parser.add_argument(
        "--execution_date",
        required=True,
        help="Partition date to validate (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--lakehouse_base_path",
        default=None,
        help="Lakehouse root path (local or s3a://). Falls back to config if omitted.",
    )
    args = parser.parse_args()

    app = GoldQualityGateApp(
        execution_date=args.execution_date, lakehouse_base_path=args.lakehouse_base_path
    )
    app.run()
