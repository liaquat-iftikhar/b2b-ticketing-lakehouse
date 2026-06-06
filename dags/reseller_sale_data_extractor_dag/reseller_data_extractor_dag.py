import os
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from common.datasets import RESELLER_SALE_DATA_EXTRACTOR_DAG_SUCCESS_DATASET

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SPARK_CONN_ID = "spark_local"

LAKEHOUSE_PATH = PROJECT_ROOT / "data" / "lakehouse"
INBOUND_PATH = PROJECT_ROOT / "data" / "inbound"
SRC_JOBS_ROOT = PROJECT_ROOT / "src" / "jobs"



default_args = {
    "owner": "data_platform_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
        dag_id="reseller_sale_data_extractor_dag",
        default_args=default_args,
        description="Extract reseller data",
        schedule="@daily",
        start_date=datetime(2026, 6, 1),
        catchup=False,
        max_active_runs=1,
        tags=["reseller", "extractor", "production"],
) as dag:
    start_gate = EmptyOperator(task_id="start_gate")
    end_gate = EmptyOperator(task_id="end_gate")

    ingest_reseller_files = SparkSubmitOperator(
        task_id="bronze_ingest_reseller_files",
        application=str(SRC_JOBS_ROOT / "reseller_sale_etl" / "bronze" / "incremental_reseller_data_extractor.py"),
        conn_id=SPARK_CONN_ID,
        env_vars={"PYTHONPATH": os.getenv("PYTHONPATH", str(PROJECT_ROOT))},
        packages="io.delta:delta-spark_2.13:4.1.0",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        },
        application_args=[
            "--source_path", str(INBOUND_PATH),
            "--execution_date", "{{ ds }}",
            "--lakehouse_base_path", str(LAKEHOUSE_PATH)
        ]
    )

    transform_resellers_silver = SparkSubmitOperator(
        task_id="silver_transform_resellers",
        application=str(SRC_JOBS_ROOT / "reseller_sale_etl" / "silver" / "reseller_data_bronze_to_silver_transformer.py"),
        conn_id=SPARK_CONN_ID,
        env_vars={"PYTHONPATH": os.getenv("PYTHONPATH", str(PROJECT_ROOT))},
        packages="io.delta:delta-spark_2.13:4.1.0",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        },
        outlets=[RESELLER_SALE_DATA_EXTRACTOR_DAG_SUCCESS_DATASET],
        application_args=[
            "--execution_date", "{{ ds }}",
            "--lakehouse_base_path", str(LAKEHOUSE_PATH)
        ]
    )

    start_gate >> ingest_reseller_files >> transform_resellers_silver >> end_gate
