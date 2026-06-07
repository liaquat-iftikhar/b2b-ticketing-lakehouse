import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from common.datasets import RESELLER_DATA_EXTRACTOR_DAG_SUCCESS_DATASET
from common.dag_config import (
    SPARK_CONN_ID, POSTGRES_CONN_ID,
    LAKEHOUSE_PATH, PROJECT_ROOT, SRC_JOBS_ROOT,
    PYSPARK_PYTHON, PYSPARK_DRIVER_PYTHON,
)

TABLE_NAME = "reseller"

default_args = {
    "owner": "data_platform_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
        dag_id="reseller_data_extractor_dag",
        default_args=default_args,
        description="Extract and ingest operational resellers data into the Lakehouse",
        schedule="@daily",
        start_date=datetime(2026, 6, 1),
        catchup=False,
        max_active_runs=1,
        tags=["reseller", "extractor", "production"],
) as dag:
    start_gate = EmptyOperator(task_id="start_gate")
    end_gate = EmptyOperator(task_id="end_gate")

    ingest_reseller_data = SparkSubmitOperator(
        task_id="bronze_ingest_reseller_data",
        application=str(SRC_JOBS_ROOT / "core" / "bronze" / "database_table_extractor.py"),
        conn_id=SPARK_CONN_ID,

        env_vars={
            "PYTHONPATH": os.getenv("PYTHONPATH", str(PROJECT_ROOT)),
            "PYSPARK_PYTHON": PYSPARK_PYTHON,
            "PYSPARK_DRIVER_PYTHON": PYSPARK_DRIVER_PYTHON,
        },

        packages="io.delta:delta-spark_2.13:4.1.0,org.postgresql:postgresql:42.7.3",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        },
        application_args=[
            "--db_host", "{{ conn.postgres_event_platform.host }}",
            "--db_port", "{{ conn.postgres_event_platform.port }}",
            "--db_user", "{{ conn.postgres_event_platform.login }}",
            "--db_password", "{{ conn.postgres_event_platform.password }}",
            "--table", TABLE_NAME,
            "--execution_date", "{{ ds }}",
            "--lakehouse_base_path", str(LAKEHOUSE_PATH)
        ]
    )

    transform_reseller_silver = SparkSubmitOperator(
        task_id="silver_transform_purchases",
        application=str(
            SRC_JOBS_ROOT / "reseller_etl" / "silver" / "reseller_data_bronze_to_silver_transformer.py"),
        conn_id=SPARK_CONN_ID,
        env_vars={"PYTHONPATH": os.getenv("PYTHONPATH", str(PROJECT_ROOT))},
        packages="io.delta:delta-spark_2.13:4.1.0",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        },
        outlets=[RESELLER_DATA_EXTRACTOR_DAG_SUCCESS_DATASET],
        application_args=[
            "--execution_date", "{{ ds }}",
            "--lakehouse_base_path", str(LAKEHOUSE_PATH)
        ]
    )

    start_gate >> ingest_reseller_data >> transform_reseller_silver >> end_gate
