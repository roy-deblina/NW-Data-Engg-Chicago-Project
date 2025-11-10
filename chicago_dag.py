from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# This is the path to your scripts ON YOUR API-VM
# Make sure this matches the folder where you cloned your git repo
BASE_PATH = "/home/azureuser/NW-Data-Engg-Chicago-Project/" 

with DAG(
    dag_id='chicago_pipeline',
    start_date=datetime(2025, 11, 1),
    schedule_interval=None, # We will trigger this manually via the API
    catchup=False
) as dag:

    # --- EXTRACT LAYER ---
    # Define all 6 extract tasks
    task_extract_taxi = BashOperator(
        task_id='extract_taxi',
        bash_command=f"python3 {BASE_PATH}extract_taxi.py"
    )
    task_extract_tnp = BashOperator(
        task_id='extract_tnp',
        bash_command=f"python3 {BASE_PATH}extract_tnp.py"
    )
    task_extract_permits = BashOperator(
        task_id='extract_permits',
        bash_command=f"python3 {BASE_PATH}extract_permits.py"
    )
    task_extract_covid = BashOperator(
        task_id='extract_covid',
        bash_command=f"python3 {BASE_PATH}extract_covid.py"
    )
    task_extract_health = BashOperator(
        task_id='extract_health',
        bash_command=f"python3 {BASE_PATH}extract_health.py"
    )
    task_extract_ccvi = BashOperator(
        task_id='extract_ccvi',
        bash_command=f"python3 {BASE_PATH}extract_ccvi.py"
    )

    # --- TRANSFORM LAYER ---
    # (Here you will add your 6 transform tasks later)
    # e.g., task_transform_taxi = BashOperator(...)
    
    # --- LOAD LAYER ---
    # (Here you will add your 4 load tasks later)
    # e.g., task_load_trips = BashOperator(...)


    # --- SET DEPENDENCIES ---
    # This just runs all 6 extract tasks in parallel.
    # We will add dependencies to the transform/load tasks later.
    extract_tasks = [
        task_extract_taxi,
        task_extract_tnp,
        task_extract_permits,
        task_extract_covid,
        task_extract_health,
        task_extract_ccvi
    ]
    
    # Example for when you add transform/load scripts:
    # task_extract_taxi >> task_transform_taxi >> task_load_trips