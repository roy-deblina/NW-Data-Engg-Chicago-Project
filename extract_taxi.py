import os
import json
from sodapy import Socrata
from azure.storage.blob import BlobServiceClient

print("Starting extraction pipeline for Taxi Trips...")

# 1. CONFIGURATION 
SODA_DOMAIN = "data.cityofchicago.org"
SODA_DATASET_ID = "wrvz-psew" # Taxi Trips dataset
DATASET_LIMIT = 1000000 # Sampling 1 million rows
BLOB_FILE_NAME = "taxi_trips_sample.json"

# Azure details
CONTAINER_NAME = "bronze"
connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')

if not connect_str:
    raise ValueError("Azure connection string not found in environment variables.")

#2. EXTRACT FROM SODA 
try:
    print(f"Connecting to SODA at {SODA_DOMAIN}...")
    client = Socrata(SODA_DOMAIN, None)
    
    print(f"Downloading {DATASET_LIMIT} rows from dataset {SODA_DATASET_ID}...")
    results = client.get(SODA_DATASET_ID, limit=DATASET_LIMIT)
    
    json_data = json.dumps(results, indent=2)
    print("...Download complete.")

except Exception as e:
    print(f"ERROR: Failed to download from SODA: {e}")
    exit(1)

# 3. UPLOAD TO AZURE BRONZE 
try:
    print(f"Connecting to Azure Blob Storage container '{CONTAINER_NAME}'...")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_FILE_NAME)
    
    print(f"Uploading {BLOB_FILE_NAME} to Bronze container...")
    blob_client.upload_blob(json_data, overwrite=True)
    
    print("...Upload complete.")
    print("Taxi Trips extraction finished successfully.")

except Exception as e:
    print(f"ERROR: Failed to upload to Azure Blob: {e}")
    exit(1)