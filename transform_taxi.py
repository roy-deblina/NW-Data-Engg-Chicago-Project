import os
import json
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import io # Needed to read/write from memory
import time

print("Starting BATCH transformation pipeline for Taxi Trips...")

# 1. CONFIGURATION
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"
BRONZE_BLOB_PREFIX = "taxi_trips_part_" 
# We now write to a "folder" in Silver, one file per chunk
SILVER_BLOB_PREFIX = "clean_taxi_trips/part_"
connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
CHUNK_SIZE = 50000 # This is for the DB temp table

# Postgres details
pg_host = os.environ.get('PG_HOST')
pg_user = os.environ.get('PG_USER')
pg_password = os.environ.get('PG_PASSWORD')
pg_db = "chicago_bi"
db_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_db}"
db_engine = create_engine(db_url)

blob_service_client = BlobServiceClient.from_connection_string(connect_str)

def clean_and_join_chunk(chunk_df):
    """
    Applies the full transformation and spatial join logic to a single DataFrame chunk.
    """
    print(f"   ...Transforming {len(chunk_df)} rows...")
    
    # 3. CLEAN & PREPARE DATA
    df = chunk_df.copy()
    df['trip_start_timestamp'] = pd.to_datetime(df.get('trip_start_timestamp'), errors='coerce')
    
    # --- THIS IS THE FIX ---
    # The column names from Socrata have 'centroid' in them
    df['pickup_latitude'] = pd.to_numeric(df.get('pickup_centroid_latitude'), errors='coerce')
    df['pickup_longitude'] = pd.to_numeric(df.get('pickup_centroid_longitude'), errors='coerce')
    df['dropoff_latitude'] = pd.to_numeric(df.get('dropoff_centroid_latitude'), errors='coerce')
    df['dropoff_longitude'] = pd.to_numeric(df.get('dropoff_centroid_longitude'), errors='coerce')
    # --- END FIX ---
    
    df.dropna(subset=['trip_id', 'trip_start_timestamp', 'pickup_latitude', 'pickup_longitude'], inplace=True)

    if df.empty:
        print("   ...Chunk is empty after cleaning.")
        return None

    locations_df = df[['pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude']].copy()
    locations_df = locations_df.reset_index().rename(columns={'index': 'temp_trip_id'})

    # 4. SPATIAL JOIN
    try:
        with db_engine.connect() as conn:
            temp_table_name = f'temp_trip_locations_{int(time.time_ns())}' # Unique temp table name
            
            locations_df.to_sql(temp_table_name, conn, if_exists='replace', index=False, chunksize=CHUNK_SIZE)

            spatial_join_query = f"""
                SELECT
                    t.temp_trip_id,
                    pickup_geo.geography_key AS pickup_geography_key,
                    dropoff_geo.geography_key AS dropoff_geography_key
                FROM
                    {temp_table_name} AS t
                LEFT JOIN
                    dim_geography AS pickup_geo ON ST_Contains(pickup_geo.geom, ST_SetSRID(ST_MakePoint(t.pickup_longitude, t.pickup_latitude), 4326))
                LEFT JOIN
                    dim_geography AS dropoff_geo ON ST_Contains(dropoff_geo.geom, ST_SetSRID(ST_MakePoint(t.dropoff_longitude, t.dropoff_latitude), 4326));
            """
            geo_keys_df = pd.read_sql(spatial_join_query, conn)
            
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(f"DROP TABLE IF EXISTS {temp_table_name};")

    except Exception as e:
        print(f"   ...ERROR: Spatial join failed for chunk: {e}")
        return None

    # 5. MERGE & FINALIZE
    df = df.reset_index().rename(columns={'index': 'temp_trip_id'})
    df_final = df.merge(geo_keys_df, on='temp_trip_id', how='left')
    
    print(f"   ...Chunk transformed. Final size: {len(df_final)} rows.")
    return df_final

def upload_chunk_to_silver(df_chunk, chunk_index):
    """
    Uploads a transformed DataFrame chunk to Azure Silver as a Parquet file.
    """
    try:
        silver_blob_name = f"{SILVER_BLOB_PREFIX}{chunk_index:04d}.parquet"
        print(f"   ...Uploading {silver_blob_name} to Silver...")
        
        output_buffer = io.BytesIO()
        df_chunk.to_parquet(output_buffer, index=False)
        output_buffer.seek(0)
        
        blob_client = blob_service_client.get_blob_client(container=SILVER_CONTAINER, blob=silver_blob_name)
        blob_client.upload_blob(output_buffer, overwrite=True)
        
        print(f"   ...Upload complete for chunk {chunk_index}.")
        
    except Exception as e:
        print(f"   ...ERROR: Failed to upload chunk {chunk_index} to Silver: {e}")

# 2. LOOP, DOWNLOAD, & PROCESS CHUNKS FROM BRONZE
try:
    print(f"Connecting to Azure container '{BRONZE_CONTAINER}'...")
    container_client = blob_service_client.get_container_client(BRONZE_CONTAINER)

    print(f"Finding all blobs with prefix '{BRONZE_BLOB_PREFIX}'...")
    blob_list = container_client.list_blobs(name_starts_with=BRONZE_BLOB_PREFIX)
    
    chunk_count = 0
    blobs_to_process = [blob for blob in blob_list if blob.name.endswith('.json')]
    
    if not blobs_to_process:
        print("ERROR: No part files found. Exiting.")
        exit(1)
        
    print(f"Found {len(blobs_to_process)} chunks to process.")

    for blob in blobs_to_process:
        chunk_count += 1
        print(f"Processing chunk {chunk_count}: {blob.name}")
        
        # 2a. DOWNLOAD CHUNK
        blob_client = blob_service_client.get_blob_client(container=BRONZE_CONTAINER, blob=blob.name)
        downloader = blob_client.download_blob()
        raw_data = json.loads(downloader.readall())
        df_chunk_raw = pd.DataFrame(raw_data)
        print(f"   ...Downloaded {len(df_chunk_raw)} rows.")

        # 2b. TRANSFORM CHUNK
        df_chunk_final = clean_and_join_chunk(df_chunk_raw)
        
        # 2c. UPLOAD CLEAN CHUNK
        if df_chunk_final is not None:
            upload_chunk_to_silver(df_chunk_final, chunk_count)

    print(f"Taxi transformation pipeline finished successfully. Processed {chunk_count} chunks.")

except Exception as e:
    print(f"ERROR: Failed during batch transform process: {e}")
    exit(1)