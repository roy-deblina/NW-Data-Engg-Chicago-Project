import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import io

print("Starting BATCH load pipeline for Fact Trips...")

# 1. CONFIGURATION
SILVER_CONTAINER = "silver"
TAXI_BLOB_PREFIX = "clean_taxi_trips/"
TNP_BLOB_PREFIX = "clean_tnp_trips/"
connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
DB_TABLE_NAME = "fact_trips"

# Postgres details
pg_host = os.environ.get('PG_HOST')
pg_user = os.environ.get('PG_USER')
pg_password = os.environ.get('PG_PASSWORD')
pg_db = "chicago_bi"
db_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_db}"
db_engine = create_engine(db_url)

# Columns to match fact_trips table
FINAL_COLUMNS = [
    'trip_id',
    'trip_start_date_key',
    'trip_end_date_key',
    'pickup_geography_key',
    'dropoff_geography_key',
    'trip_source',
    'trip_start_timestamp',
    'trip_end_timestamp',
    'trip_duration_seconds',
    'trip_distance_miles',
    'fare',
    'tip',
    'trip_total'
]

def process_and_load_chunk(df_chunk, trip_source, load_method):
    """
    Prepares a single chunk and loads it into the database.
    """
    print(f"   ...Preparing {len(df_chunk)} rows from '{trip_source}' source...")
    df_chunk['trip_source'] = trip_source
    
    # Create date keys
    df_chunk['trip_start_timestamp'] = pd.to_datetime(df_chunk['trip_start_timestamp'])
    df_chunk['trip_start_date_key'] = df_chunk['trip_start_timestamp'].dt.strftime('%Y%m%d').astype(int)
        
    df_chunk['trip_end_timestamp'] = pd.to_datetime(df_chunk.get('trip_end_timestamp'), errors='coerce')
    df_chunk['trip_end_date_key'] = df_chunk['trip_end_timestamp'].dt.strftime('%Y%m%d')
    df_chunk['trip_end_date_key'].fillna(0, inplace=True)
    df_chunk['trip_end_date_key'] = df_chunk['trip_end_date_key'].astype(int)

    # Filter df to only these columns, handling missing ones
    final_df = df_chunk.reindex(columns=FINAL_COLUMNS)
    
    # Load to Gold
    print(f"   ...Loading chunk to '{DB_TABLE_NAME}' (method: {load_method})...")
    final_df.to_sql(
        DB_TABLE_NAME,
        db_engine,
        if_exists=load_method, 
        index=False,
        method='multi', # Use 'multi' for faster inserts
        chunksize=10000  # Load 10,000 rows at a time
    )
    print("   ...Chunk loaded successfully.")

try:
    print(f"Connecting to Azure container '{SILVER_CONTAINER}'...")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(SILVER_CONTAINER)
    
    is_first_chunk_overall = True # This will control the 'replace' logic
    
    # --- STAGE 1: PROCESS TAXI TRIPS ---
    print(f"--- Processing Taxi Trips (Prefix: {TAXI_BLOB_PREFIX}) ---")
    taxi_blob_list = container_client.list_blobs(name_starts_with=TAXI_BLOB_PREFIX)
    
    taxi_files = [blob for blob in taxi_blob_list if blob.name.endswith('.parquet')]
    if not taxi_files:
        print("WARNING: No clean taxi part-files found.")
    
    for blob in taxi_files:
        print(f"...Processing {blob.name}")
        
        # Download chunk
        blob_client = blob_service_client.get_blob_client(container=SILVER_CONTAINER, blob=blob.name)
        downloader = blob_client.download_blob()
        data_bytes = downloader.readall()
        df = pd.read_parquet(io.BytesIO(data_bytes))
        print(f"   ...Downloaded {len(df)} rows.")

        # Determine load method
        if is_first_chunk_overall:
            load_method = 'replace'
            is_first_chunk_overall = False
        else:
            load_method = 'append'
            
        # Process and load this chunk
        process_and_load_chunk(df, 'Taxi', load_method)

    # --- STAGE 2: PROCESS TNP TRIPS ---
    print(f"--- Processing TNP Trips (Prefix: {TNP_BLOB_PREFIX}) ---")
    tnp_blob_list = container_client.list_blobs(name_starts_with=TNP_BLOB_PREFIX)

    tnp_files = [blob for blob in tnp_blob_list if blob.name.endswith('.parquet')]
    if not tnp_files:
        print("WARNING: No clean TNP part-files found.")

    for blob in tnp_files:
        print(f"...Processing {blob.name}")
        
        # Download chunk
        blob_client = blob_service_client.get_blob_client(container=SILVER_CONTAINER, blob=blob.name)
        downloader = blob_client.download_blob()
        data_bytes = downloader.readall()
        df = pd.read_parquet(io.BytesIO(data_bytes))
        print(f"   ...Downloaded {len(df)} rows.")

        # Determine load method
        if is_first_chunk_overall:
            load_method = 'replace' # This would only happen if no taxi files were found
            is_first_chunk_overall = False
        else:
            load_method = 'append'
            
        # Process and load this chunk
        process_and_load_chunk(df, 'TNP', load_method)

    if is_first_chunk_overall:
        print("WARNING: No parquet files were found for EITHER taxi or tnp. No data was loaded.")
    else:
        print(f"Load pipeline for Fact Trips finished successfully.")

except Exception as e:
    print(f"ERROR: Failed during batch load process: {e}")
    exit(1)