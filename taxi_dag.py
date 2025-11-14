from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# This is the path to your scripts ON YOUR API-VM
# We assume your GitHub repo is cloned in /home/azureuser/
BASE_PATH = "/home/azureuser/NW-Data-Engg-Chicago-Project/" # Make sure this path is correct

# --- Default Arguments ---
# These args will be applied to all your tasks
default_args = {
    'owner': 'Group7',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, # If a task fails, retry it 1 time
    'retry_delay': timedelta(minutes=5), # Wait 5 minutes between retries
}

with DAG(
    dag_id='taxi_transform_pipeline',
    default_args=default_args,
    description='Taxi ETL pipeline for Chicago BI project',
    start_date=datetime(2025, 12, 1),
    schedule=None, # We will trigger this manually via the API
    catchup=False,
    tags=['chicago', 'bi', 'project']
) as dag:


    # --- 2. TRANSFORM LAYER ---
    # All transform tasks are active and will run.
    
    task_transform_taxi = BashOperator(
        task_id='transform_taxi',
        bash_command=f"python3 {BASE_PATH}transform_taxi.py"
    )
    
    task_transform_taxi


    