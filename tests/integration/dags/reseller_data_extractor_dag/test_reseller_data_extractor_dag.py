"""
Integration tests for reseller_data_extractor_dag.

These tests validate DAG structure, task wiring, and operator configuration
without executing any Spark jobs or touching a real database. They load the
DAG through Airflow's DagBag — the same mechanism Airflow uses in production
— which exercises the full import chain:
    dag_config → ConfigLoader → Dynaconf → settings.toml

IMPORTANT: Airflow env vars and PYTHONPATH must be set BEFORE any Airflow
module is imported, because SQLAlchemy caches the DB engine at import time.
This file sets them at the top, before any airflow.* imports.

Run with:
    PYTHONPATH=. python -m unittest \
        tests.integration.dags.reseller_data_extractor_dag.test_reseller_data_extractor_dag -v

    # or discover all integration tests:
    PYTHONPATH=. python -m unittest discover -s tests/integration -p "test_*.py" -v
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path

# ── Airflow requires env vars to be set before its first import ──────────────
_AIRFLOW_TMP_DIR = tempfile.mkdtemp(prefix="airflow_home_test_")
os.environ["AIRFLOW_HOME"] = _AIRFLOW_TMP_DIR
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "false"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = (
    f"sqlite:///{_AIRFLOW_TMP_DIR}/airflow.db"
)

# ── Ensure project root and dags/ are importable (mirrors Airflow container) ─
_PROJECT_ROOT = (
    Path(__file__).resolve().parents[4]
)  # …/tests/integration/dags/<dag>/file → 4 levels up
_DAGS_FOLDER = _PROJECT_ROOT / "dags"

for _p in (str(_PROJECT_ROOT), str(_DAGS_FOLDER)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(_DAGS_FOLDER))

# ── Now it is safe to import Airflow ─────────────────────────────────────────
from airflow.models import DagBag  # noqa: E402

# ── Constants ─────────────────────────────────────────────────────────────────
DAG_ID = "reseller_data_extractor_dag"
DELTA_PACKAGES = "io.delta:delta-spark_2.13:4.1.0"
JDBC_PACKAGE = "org.postgresql:postgresql:42.7.3"
DELTA_EXTENSIONS_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


def setUpModule():
    """Initialise the Airflow metadata DB schema once for the entire module."""
    from airflow.utils.db import initdb

    initdb()


class TestResellerDataExtractorDagLoad(unittest.TestCase):
    """Validates that the DAG is importable and free of syntax / import errors."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.dagbag = DagBag(dag_folder=str(_DAGS_FOLDER), include_examples=False)

    def test_no_import_errors(self) -> None:
        """DagBag must not report any import errors for the DAG module."""
        errors = {
            k: v
            for k, v in self.dagbag.import_errors.items()
            if DAG_ID in k or "reseller_data_extract" in k
        }
        self.assertEqual(errors, {}, msg=f"DAG import errors detected: {errors}")

    def test_dag_is_registered(self) -> None:
        """DAG ID must be present in the loaded bag."""
        self.assertIn(DAG_ID, self.dagbag.dags, msg=f"'{DAG_ID}' not found in DagBag")


class TestResellerDataExtractorDagMetadata(unittest.TestCase):
    """Validates top-level DAG properties."""

    @classmethod
    def setUpClass(cls) -> None:
        dagbag = DagBag(dag_folder=str(_DAGS_FOLDER), include_examples=False)
        cls.dag = dagbag.dags[DAG_ID]

    def test_dag_id(self) -> None:
        self.assertEqual(self.dag.dag_id, DAG_ID)

    def test_schedule_is_daily(self) -> None:
        # Airflow 3 uses .schedule; .schedule_interval was removed.
        self.assertEqual(self.dag.schedule, "@daily")

    def test_catchup_is_disabled(self) -> None:
        self.assertFalse(self.dag.catchup)

    def test_max_active_runs(self) -> None:
        self.assertEqual(self.dag.max_active_runs, 1)

    def test_owner(self) -> None:
        self.assertEqual(self.dag.default_args["owner"], "data_platform_team")


class TestResellerDataExtractorDagStructure(unittest.TestCase):
    """Validates task presence and the linear dependency chain."""

    EXPECTED_TASK_IDS = {
        "start_gate",
        "bronze_ingest_reseller_data",
        "silver_transform_purchases",
        "end_gate",
    }

    @classmethod
    def setUpClass(cls) -> None:
        dagbag = DagBag(dag_folder=str(_DAGS_FOLDER), include_examples=False)
        cls.dag = dagbag.dags[DAG_ID]

    def test_task_count(self) -> None:
        self.assertEqual(len(self.dag.task_ids), len(self.EXPECTED_TASK_IDS))

    def test_all_task_ids_present(self) -> None:
        self.assertEqual(set(self.dag.task_ids), self.EXPECTED_TASK_IDS)

    def test_dependency_chain(self) -> None:
        """start_gate → bronze_ingest_reseller_data → silver_transform_purchases → end_gate"""

        def downstream_ids(task_id: str) -> set:
            return {t.task_id for t in self.dag.get_task(task_id).downstream_list}

        self.assertIn("bronze_ingest_reseller_data", downstream_ids("start_gate"))
        self.assertIn(
            "silver_transform_purchases", downstream_ids("bronze_ingest_reseller_data")
        )
        self.assertIn("end_gate", downstream_ids("silver_transform_purchases"))

    def test_start_gate_has_no_upstream(self) -> None:
        self.assertEqual(self.dag.get_task("start_gate").upstream_list, [])

    def test_end_gate_has_no_downstream(self) -> None:
        self.assertEqual(self.dag.get_task("end_gate").downstream_list, [])


class TestBronzeIngestResellerTask(unittest.TestCase):
    """Validates the SparkSubmitOperator that ingests the reseller table into Bronze."""

    @classmethod
    def setUpClass(cls) -> None:
        dagbag = DagBag(dag_folder=str(_DAGS_FOLDER), include_examples=False)
        cls.task = dagbag.dags[DAG_ID].get_task("bronze_ingest_reseller_data")

    def test_operator_type(self) -> None:
        from airflow.providers.apache.spark.operators.spark_submit import (
            SparkSubmitOperator,
        )

        self.assertIsInstance(self.task, SparkSubmitOperator)

    def test_conn_id(self) -> None:
        """Must reference the central spark_conn_id from dag_config."""
        self.assertEqual(self.task._conn_id, "spark_local")

    def test_application_points_to_database_extractor(self) -> None:
        self.assertTrue(
            self.task.application.endswith("database_table_extractor.py"),
            msg=f"Unexpected application path: {self.task.application}",
        )

    def test_packages_include_delta_and_jdbc(self) -> None:
        packages = self.task.packages or ""
        self.assertIn(DELTA_PACKAGES, packages)
        self.assertIn(JDBC_PACKAGE, packages)

    def test_delta_spark_conf(self) -> None:
        conf = self.task.conf or {}
        for key, value in DELTA_EXTENSIONS_CONF.items():
            self.assertEqual(
                conf.get(key), value, msg=f"Missing or wrong conf key: {key}"
            )

    def test_application_args_include_table_reseller(self) -> None:
        """--table reseller must be present so the extractor targets the right source table."""
        args = self.task.application_args or []
        self.assertIn("--table", args)
        self.assertEqual(args[args.index("--table") + 1], "reseller")

    def test_application_args_include_execution_date_template(self) -> None:
        args = self.task.application_args or []
        self.assertIn("--execution_date", args)
        self.assertIn("{{ ds }}", args[args.index("--execution_date") + 1])

    def test_application_args_include_lakehouse_base_path(self) -> None:
        self.assertIn("--lakehouse_base_path", self.task.application_args or [])

    def test_db_connection_args_use_jinja_templates(self) -> None:
        """Credentials must come from Airflow connection templating, not be hardcoded."""
        args = " ".join(self.task.application_args or [])
        self.assertIn("conn.postgres_event_platform.host", args)
        self.assertIn("conn.postgres_event_platform.login", args)
        self.assertIn("conn.postgres_event_platform.password", args)


class TestSilverTransformResellerTask(unittest.TestCase):
    """Validates the SparkSubmitOperator that transforms Bronze → Silver."""

    @classmethod
    def setUpClass(cls) -> None:
        dagbag = DagBag(dag_folder=str(_DAGS_FOLDER), include_examples=False)
        cls.task = dagbag.dags[DAG_ID].get_task("silver_transform_purchases")

    def test_operator_type(self) -> None:
        from airflow.providers.apache.spark.operators.spark_submit import (
            SparkSubmitOperator,
        )

        self.assertIsInstance(self.task, SparkSubmitOperator)

    def test_conn_id(self) -> None:
        self.assertEqual(self.task._conn_id, "spark_local")

    def test_application_points_to_reseller_silver_transformer(self) -> None:
        self.assertTrue(
            self.task.application.endswith(
                "reseller_data_bronze_to_silver_transformer.py"
            ),
            msg=f"Unexpected application path: {self.task.application}",
        )

    def test_packages_include_delta(self) -> None:
        self.assertIn(DELTA_PACKAGES, self.task.packages or "")

    def test_delta_spark_conf(self) -> None:
        conf = self.task.conf or {}
        for key, value in DELTA_EXTENSIONS_CONF.items():
            self.assertEqual(
                conf.get(key), value, msg=f"Missing or wrong conf key: {key}"
            )

    def test_application_args_include_execution_date_template(self) -> None:
        args = self.task.application_args or []
        self.assertIn("--execution_date", args)
        self.assertIn("{{ ds }}", args[args.index("--execution_date") + 1])

    def test_application_args_include_lakehouse_base_path(self) -> None:
        self.assertIn("--lakehouse_base_path", self.task.application_args or [])

    def test_success_dataset_outlet_is_registered(self) -> None:
        """Silver task must emit the Dataset that downstream DAGs subscribe to."""
        outlet_uris = {str(o.uri) for o in (self.task.outlets or [])}
        self.assertTrue(
            any("reseller_data_extractor_dag" in uri for uri in outlet_uris),
            msg=f"Expected reseller success Dataset outlet, got: {outlet_uris}",
        )


if __name__ == "__main__":
    unittest.main()
