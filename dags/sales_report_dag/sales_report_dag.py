import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from common.datasets import SALES_REPORT_DAG_SUCCESS_DATASET
from common.dag_config import (
    SPARK_CONN_ID,
    LAKEHOUSE_PATH, PROJECT_ROOT, SRC_JOBS_ROOT,
)

default_args = {
    "owner": "data_platform_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
        dag_id="sales_report_dag",
        default_args=default_args,
        description="Generate the Sales Report",
        schedule="@daily",
        start_date=datetime(2026, 6, 1),
        catchup=False,
        max_active_runs=1,
        tags=["sales_report", "fact", "production"],
) as dag:
    start_gate = EmptyOperator(task_id="start_gate")
    end_gate = EmptyOperator(task_id="end_gate")

    generate_sales_report = SparkSubmitOperator(
        task_id="gold_generate_sales_report",
        application=str(
            SRC_JOBS_ROOT / "sales_report_etl" / "gold" / "sales_report_etl.py"),
        conn_id=SPARK_CONN_ID,
        env_vars={"PYTHONPATH": os.getenv("PYTHONPATH", str(PROJECT_ROOT))},
        packages="io.delta:delta-spark_2.13:4.1.0",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        },
        outlets=[SALES_REPORT_DAG_SUCCESS_DATASET],
        application_args=[
            "--execution_date", "{{ ds }}",
            "--lakehouse_base_path", str(LAKEHOUSE_PATH)
        ]
    )

    start_gate >> generate_sales_report >> end_gate
