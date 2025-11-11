"""Trigger the chicago_pipeline DAG."""
import subprocess
import sys

# Initialize the Airflow database
print("Initializing Airflow database...")
result = subprocess.run(['airflow', 'db', 'migrate'], capture_output=True, text=True)
if result.returncode != 0:
    print(f"Error migrating database: {result.stderr}")
    sys.exit(1)

# Trigger the DAG
print("Triggering chicago_pipeline DAG...")
result = subprocess.run(['airflow', 'dags', 'trigger', 'chicago_pipeline'], capture_output=True, text=True)
if result.returncode != 0:
    print(f"Error triggering DAG: {result.stderr}")
    sys.exit(1)

print(result.stdout)
print("DAG triggered successfully!")
print("\nTo monitor the DAG run, use:")
print("  airflow dags list-runs --dag-id chicago_pipeline")
print("  airflow tasks list chicago_pipeline")
