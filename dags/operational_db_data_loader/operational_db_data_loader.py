from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from common.datasets import OPERATIONAL_DB_DATA_LOADER_DAG_SUCCESS_DATASET
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
        dag_id="operational_db_data_loader",
        default_args=default_args,
        description="Dag to generate operational db data",
        schedule=None,
        start_date=datetime(2026, 6, 1),
        catchup=False,
        max_active_runs=1,
        tags=["operational", "db", "data", "generator", "local"],  # Fixed missing comma string bug
) as dag:

    start_gate = EmptyOperator(task_id="start_gate")
    end_gate = EmptyOperator(task_id="end_gate")

    load_operational_db = BashOperator(
        task_id="load_operational_db",
        cwd="/opt/airflow",
        outlets=[OPERATIONAL_DB_DATA_LOADER_DAG_SUCCESS_DATASET],
        bash_command=(
            "echo 'Starting operational orchestrator process loop...' && "
            "python -m src.utils.generator.operational_db.orchestrator && "
            "echo 'Operational database script completed successfully.'"
        ),
    )

    start_gate >> load_operational_db >> end_gate
