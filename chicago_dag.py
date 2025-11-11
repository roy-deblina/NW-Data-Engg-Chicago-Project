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
    dag_id='chicago_pipeline',
    default_args=default_args,
    description='Full ETL pipeline for Chicago BI project',
    start_date=datetime(2025, 11, 1),
    schedule_interval=None, # We will trigger this manually via the API
    catchup=False,
    tags=['chicago', 'bi', 'project']
) as dag:

    # --- 1. EXTRACT LAYER ---
    # All extract tasks are commented out.
    
    # task_extract_taxi = BashOperator(
    #     task_id='extract_taxi',
    #     bash_command=f"python3 {BASE_PATH}extract_taxi.py"
    # )
    
    # task_extract_tnp = BashOperator(
    #     task_id='extract_tnp',
    #     bash_command=f"python3 {BASE_PATH}extract_tnp.py"
    # )
    
    # task_extract_permits = BashOperator(
    #     task_id='extract_permits',
    #     bash_command=f"python3 {BASE_PATH}extract_permits.py"
    # )
    
    # task_extract_covid = BashOperator(
    #     task_id='extract_covid',
    #     bash_command=f"python3 {BASE_PATH}extract_covid.py"
    # )
    # task_extract_health = BashOperator(
    #     task_id='extract_health',
    #     bash_command=f"python3 {BASE_PATH}extract_health.py"
    # )
    # task_extract_ccvi = BashOperator(
    #     task_id='extract_ccvi',
    #     bash_command=f"python3 {BASE_PATH}extract_ccvi.py"
    # )

    # --- 2. TRANSFORM LAYER ---
    # All transform tasks are active and will run.
    
    task_transform_taxi = BashOperator(
        task_id='transform_taxi',
        bash_command=f"python3 {BASE_PATH}transform_taxi.py"
    )
    
    task_transform_tnp = BashOperator(
        task_id='transform_tnp',
        bash_command=f"python3 {BASE_PATH}transform_tnp.py"
    )
    
    task_transform_permits = BashOperator(
        task_id='transform_permits',
        bash_command=f"python3 {BASE_PATH}transform_permits.py"
    )
    
    task_transform_covid = BashOperator(
         task_id='transform_covid',
         bash_command=f"python3 {BASE_PATH}transform_covid.py"
    )
    task_transform_health = BashOperator(
     task_id='transform_health',
     bash_command=f"python3 {BASE_PATH}transform_health.py"
    )
    task_transform_ccvi = BashOperator(
        task_id='transform_ccvi',
        bash_command=f"python3 {BASE_PATH}transform_ccvi.py"
    )

    # --- 3. LOAD LAYER ---
    # All load tasks are active and will run.
    
    task_load_trips = BashOperator(
        task_id='load_trips',
        bash_command=f"python3 {BASE_PATH}load_trips.py"
    )
    
    task_load_permits = BashOperator(
        task_id='load_permits',
        bash_command=f"python3 {BASE_PATH}load_permits.py"
    )
    
    task_load_covid = BashOperator(
        task_id='load_covid',
        bash_command=f"python3 {BASE_PATH}load_covid.py"
    )
    task_load_health = BashOperator(
        task_id='load_health',
        bash_command=f"python3 {BASE_PATH}load_health.py"
    )
    
    # --- 4. SET DEPENDENCIES (The "Master Plan") ---
    # Since all extract tasks are commented, all transform
    # tasks will run immediately, in parallel.
    
    # Trips Pipeline:
    # BOTH transform tasks must finish before the load_trips task can run
    [task_transform_taxi, task_transform_tnp] >> task_load_trips
    
    # Permits Pipeline:
    task_transform_permits >> task_load_permits
    
    # COVID Pipeline:
    task_transform_covid >> task_load_covid
    
    # Health/Socioeconomic Pipeline:
    # BOTH transform tasks must finish before the load_health task can run
    [task_transform_health, task_transform_ccvi] >> task_load_health