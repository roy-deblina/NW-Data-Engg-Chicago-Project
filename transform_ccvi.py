import os
import json
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import io

print("Starting transformation pipeline for CCVI...")

# 1. CONFIGURATION
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"
BRONZE_BLOB_NAME = "ccvi.json" # CHANGED
SILVER_BLOB_NAME = "clean_ccvi.parquet" #  CHANGED
connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')

# Postgres details
pg_host = os.environ.get('PG_HOST')
pg_user = os.environ.get('PG_USER')
pg_password = os.environ.get('PG_PASSWORD')
pg_db = "chicago_bi"
db_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_db}"
db_engine = create_engine(db_url)

# 2. DOWNLOAD FROM BRONZE
try:
    print(f"Downloading {BRONZE_BLOB_NAME} from Bronze...")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=BRONZE_CONTAINER, blob=BRONZE_BLOB_NAME)
    
    downloader = blob_client.download_blob()
    raw_data = json.loads(downloader.readall())
    df = pd.DataFrame(raw_data)
    print(f"...Downloaded {len(df)} rows.")

except Exception as e:
    print(f"ERROR: Failed to download from Bronze: {e}")
    exit(1)

# 3. LOAD GEOGRAPHY KEY 
print("Loading geography dimension...")
try:
    # Load the geography table from Postgres
    geo_df = pd.read_sql("SELECT geography_key, community_area_number FROM dim_geography", db_engine)
    # Convert comm area number to numeric for merging
    geo_df['community_area_number'] = pd.to_numeric(geo_df['community_area_number'], errors='coerce')
    # Remove duplicates
    geo_df = geo_df.drop_duplicates(subset=['community_area_number']).copy()
except Exception as e:
    print(f"ERROR: Failed to load geography dimension: {e}")
    exit(1)


# 4. CLEAN & MERGE (NO SPATIAL JOIN NEEDED)
print("Cleaning and merging data...")
# Clean the CCVI data
df['community_area_number'] = pd.to_numeric(df.get('community_area'), errors='coerce')
df.rename(columns={'ccvi_category': 'ccvi_category_name', 'ccvi_score': 'ccvi_score_value'}, inplace=True)

# Drop rows with no valid community area
df.dropna(subset=['community_area_number'], inplace=True)

# This is the "join"
df_final = df.merge(geo_df, on='community_area_number', how='left')

print("...Merge complete.")

# 5. UPLOAD TO SILVER
try:
    print(f"Uploading {SILVER_BLOB_NAME} to Silver...")
    output_buffer = io.BytesIO()
    df_final.to_parquet(output_buffer, index=False)
    output_buffer.seek(0)
    
    blob_client = blob_service_client.get_blob_client(container=SILVER_CONTAINER, blob=SILVER_BLOB_NAME)
    blob_client.upload_blob(output_buffer, overwrite=True)
    
    print("...Upload complete.")
    print("CCVI transformation pipeline finished successfully.")
    
except Exception as e:
    print(f"ERROR: Failed to upload to Silver: {e}")
    exit(1)