import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import io

print("Starting BATCH load pipeline for Fact Permits...")

# 1. CONFIGURATION
SILVER_CONTAINER = "silver"
# We now look for a *prefix* (folder) in Silver
SILVER_BLOB_PREFIX = "clean_building_permits/"
connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
DB_TABLE_NAME = "fact_permits"

# Postgres details
pg_host = os.environ.get('PG_HOST')
pg_user = os.environ.get('PG_USER')
pg_password = os.environ.get('PG_PASSWORD')
pg_db = "chicago_bi"
db_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_db}"
db_engine = create_engine(db_url)

try:
    print(f"Connecting to Azure container '{SILVER_CONTAINER}'...")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(SILVER_CONTAINER)

    print(f"Finding all blobs with prefix '{SILVER_BLOB_PREFIX}'...")
    blob_list = container_client.list_blobs(name_starts_with=SILVER_BLOB_PREFIX)
    
    first_chunk = True # Flag to handle table replacement
    
    # 2. LOOP, DOWNLOAD, & LOAD CHUNKS
    for blob in blob_list:
        if not blob.name.endswith('.parquet'):
            continue
            
        print(f"...Processing {blob.name}")
        
        # 2a. DOWNLOAD CHUNK FROM SILVER
        blob_client = blob_service_client.get_blob_client(container=SILVER_CONTAINER, blob=blob.name)
        downloader = blob_client.download_blob()
        clean_data_bytes = downloader.readall()
        df = pd.read_parquet(io.BytesIO(clean_data_bytes))
        print(f"   ...Downloaded {len(df)} rows.")

        # 3. PREPARE CHUNK FOR GOLD
        df['issue_date'] = pd.to_datetime(df.get('issue_date'))
        df['issue_date_key'] = df['issue_date'].dt.strftime('%Y%m%d').astype(int)

        df.rename(columns={
            'id': 'permit_key',
            'permit_type': 'permit_type',
            'work_description': 'work_description',
            'reported_cost': 'reported_cost'
        }, inplace=True)

        final_columns = [
            'permit_key',
            'issue_date_key',
            'geography_key',
            'permit_type',
            'work_description',
            'reported_cost'
        ]
        
        # Use reindex to ensure all columns are present, fill missing with None (or NaN)
        final_df = df.reindex(columns=final_columns)
        
        # 4. LOAD CHUNK TO GOLD
        
        # For the first chunk, we REPLACE the table
        if first_chunk:
            print(f"   ...Replacing table '{DB_TABLE_NAME}' with first chunk.")
            load_method = 'replace'
            first_chunk = False
        # For all other chunks, we APPEND
        else:
            print(f"   ...Appending chunk to table '{DB_TABLE_NAME}'.")
            load_method = 'append'

        final_df.to_sql(
            DB_TABLE_NAME,
            db_engine,
            if_exists=load_method, 
            index=False,
            method='multi',
            chunksize=10000 # chunksize for to_sql is about DB insertion, not memory
        )
        print(f"   ...Chunk loaded successfully.")

    if first_chunk:
        print("WARNING: No parquet files were found. No data was loaded.")
    else:
        print(f"Load pipeline for Fact Permits finished successfully.")
        
except Exception as e:
    print(f"ERROR: Failed during batch load process: {e}")
    exit(1)