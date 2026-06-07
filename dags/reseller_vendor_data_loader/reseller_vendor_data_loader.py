from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from common.datasets import RESELLER_VENDOR_DATA_LOADER_DAG_SUCCESS_DATASET
from common.dag_config import POSTGRES_CONN_ID, LAKEHOUSE_PATH, PROJECT_ROOT


LAKEHOUSE_PATH = PROJECT_ROOT / "data" / "lakehouse"
SRC_JOBS_ROOT = PROJECT_ROOT / "src" / "jobs"

default_args = {
    "owner": "data_platform_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
        dag_id="reseller_vendor_data_loader",
        default_args=default_args,
        description="Dag to generate operational db data",
        schedule=None,
        start_date=datetime(2026, 6, 1),
        catchup=False,
        max_active_runs=1,
        tags=["reseller", "vendor", "data", "generator", "local"],
) as dag:
    start_gate = EmptyOperator(task_id="start_gate")
    end_gate = EmptyOperator(task_id="end_gate")

    load_vendor_data = BashOperator(
        task_id="load_vendor_data",
        cwd="/opt/airflow",
        outlets=[RESELLER_VENDOR_DATA_LOADER_DAG_SUCCESS_DATASET],
        bash_command=(
            "echo 'Starting reseller sales data orchestrator process loop...' && "
            "python -m src.utils.generator.reseller_vendor_data.orchestrator && "
            "echo 'Reseller vendor data script completed successfully.'"
        ),
    )

    start_gate >> load_vendor_data >> end_gate
