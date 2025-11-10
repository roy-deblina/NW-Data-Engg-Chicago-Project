from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import os


BASE_PATH = os.getenv("BASE_PATH", "/home/azureuser/NW-Data-Engg-Chicago-Project/")

default_args = {
    "owner": "group7",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="chicago_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    schedule=None,
    catchup=False,
) as dag:

    # Extract tasks
    task_extract_taxi = BashOperator(
        task_id="extract_taxi", bash_command=f"python3 {BASE_PATH}extract_taxi.py"
    )

    task_extract_tnp = BashOperator(
        task_id="extract_tnp", bash_command=f"python3 {BASE_PATH}extract_tnp.py"
    )

    task_extract_permits = BashOperator(
        task_id="extract_permits", bash_command=f"python3 {BASE_PATH}extract_permits.py"
    )

    task_extract_covid = BashOperator(
        task_id="extract_covid", bash_command=f"python3 {BASE_PATH}extract_covid.py"
    )

    task_extract_health = BashOperator(
        task_id="extract_health", bash_command=f"python3 {BASE_PATH}extract_health.py"
    )

    task_extract_ccvi = BashOperator(
        task_id="extract_ccvi", bash_command=f"python3 {BASE_PATH}extract_ccvi.py"
    )

    # Run extract tasks in parallel for now (no downstream transforms defined yet)
    # Placeholders for future transform/load tasks may be added later.
    extract_tasks = [
        task_extract_taxi,
        task_extract_tnp,
        task_extract_permits,
        task_extract_covid,
        task_extract_health,
        task_extract_ccvi,
    ]
