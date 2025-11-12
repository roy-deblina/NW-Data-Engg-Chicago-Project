import os
import ijson  # Incremental JSON parser
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import io
import time
import tempfile # Import tempfile
import sys # Import sys for stdout flushing
from tqdm import tqdm # --- NEW: Import tqdm ---

print("Starting BATCH transformation pipeline for TNP Trips...")

# 1. CONFIGURATION
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"
BRONZE_BLOB_NAME = "tnp_trips_sample.json"
SILVER_BLOB_PREFIX = "clean_tnp_trips/part_"
connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
CHUNK_SIZE = 50000  # Process 50,000 records at a time

# Postgres details
pg_host = os.environ.get('PG_HOST')
pg_user = os.environ.get('PG_USER')
pg_password = os.environ.get('PG_PASSWORD')
pg_db = "chicago_bi"
db_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_db}"
db_engine = create_engine(db_url)

blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# --- REMOVED: Custom DownloadProgressLogger class ---

def clean_and_join_chunk(chunk_df):
    """
    Applies the full transformation and spatial join logic to a single DataFrame chunk.
    """
    if chunk_df.empty:
        print("   ...Chunk is empty.")
        return None
        
    print(f"   ...Transforming {len(chunk_df)} rows...")
    
    # 3. CLEAN & PREPARE DATA
    df = chunk_df.copy()
    df['trip_start_timestamp'] = pd.to_datetime(df.get('trip_start_timestamp'), errors='coerce')
    
    df['pickup_latitude'] = pd.to_numeric(df.get('pickup_centroid_latitude'), errors='coerce')
    df['pickup_longitude'] = pd.to_numeric(df.get('pickup_centroid_longitude'), errors='coerce')
    df['dropoff_latitude'] = pd.to_numeric(df.get('dropoff_centroid_latitude'), errors='coerce')
    df['dropoff_longitude'] = pd.to_numeric(df.get('dropoff_centroid_longitude'), errors='coerce')

    df.dropna(subset=['trip_id', 'trip_start_timestamp', 'pickup_latitude', 'pickup_longitude'], inplace=True)
    
    if df.empty:
        print("   ...Chunk is empty after cleaning.")
        return None

    locations_df = df[['pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude']].copy()
    locations_df = locations_df.reset_index().rename(columns={'index': 'temp_trip_id'})

    # 4. SPATIAL JOIN
    try:
        with db_engine.connect() as conn:
            temp_table_name = f'temp_tnp_locations_{int(time.time_ns())}' # Unique temp table name
            
            locations_df.to_sql(temp_table_name, conn, if_exists='replace', index=False, chunksize=10000)

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

# 2. STREAM DOWNLOAD FROM BRONZE & PROCESS IN CHUNKS
tmp_file_path = None
try:
    print(f"Downloading {BRONZE_BLOB_NAME} to a temporary local file...")
    blob_client = blob_service_client.get_blob_client(container=BRONZE_CONTAINER, blob=BRONZE_BLOB_NAME)
    
    # --- THIS IS THE FIX ---
    # 1. Get total size for the progress bar
    blob_properties = blob_client.get_blob_properties()
    total_size = blob_properties.size
    print(f"Total file size: {total_size / (1024*1024):.2f} MB")

    # 2. Get the downloader and its chunks
    downloader = blob_client.download_blob()
    chunk_iterable = downloader.chunks()

    # 3. Create a temp file on disk (binary write mode)
    with tempfile.NamedTemporaryFile(delete=False, mode='wb') as tmp_file:
        
        # 4. Wrap the chunk iterable with tqdm for a progress bar
        progress_bar = tqdm(
            iterable=chunk_iterable,
            total=total_size,           # Total size in bytes
            unit='B',                   # Unit is bytes
            unit_scale=True,            # Auto-convert to KB, MB, GB
            desc=f"Downloading {BRONZE_BLOB_NAME}",
            file=sys.stderr,            # <-- THIS IS THE FIX: Write to stderr
            mininterval=5,              # Update at most every 5 seconds
            ascii=True,                 # Safe for logs
            leave=True                  # Leave the bar after finishing
        )
        
        # 5. Iterate and write to file
        for chunk in progress_bar:
            tmp_file.write(chunk)
        
        tmp_file_path = tmp_file.name # Save the path for later
    
    print(f"\n...Download to temp file complete: {tmp_file_path}")
    # --- END FIX ---
    
    # 6. Stream from the local temp file (which ijson can handle)
    print(f"Streaming from local file: {tmp_file_path}")
    with open(tmp_file_path, 'rb') as local_stream:
        # Use ijson.items to iterate over the array
        tnp_stream = ijson.items(local_stream, 'item')
        
        chunk_data = []
        chunk_count = 0
        
        for tnp_record in tnp_stream:
            chunk_data.append(tnp_record)
            
            if len(chunk_data) >= CHUNK_SIZE:
                chunk_count += 1
                print(f"Processing chunk {chunk_count}...")
                df_chunk_raw = pd.DataFrame(chunk_data)
                chunk_data = [] # Clear memory
                
                df_chunk_final = clean_and_join_chunk(df_chunk_raw)
                
                if df_chunk_final is not None:
                    upload_chunk_to_silver(df_chunk_final, chunk_count)

        # Process any remaining records in the last chunk
        if chunk_data:
            chunk_count += 1
            print(f"Processing final chunk {chunk_count}...")
            df_chunk_raw = pd.DataFrame(chunk_data)
            chunk_data = []
            
            df_chunk_final = clean_and_join_chunk(df_chunk_raw)
            
            if df_chunk_final is not None:
                upload_chunk_to_silver(df_chunk_final, chunk_count)

    if chunk_count == 0:
        print("WARNING: No data was found in the JSON stream.")
        
    print(f"TNP transformation pipeline finished successfully. Processed {chunk_count} chunks.")

except Exception as e:
    print(f"ERROR: Failed to stream or process from Bronze: {e}")
    exit(1)
finally:
    # 7. Clean up the temp file
    if tmp_file_path and os.path.exists(tmp_file_path):
        print(f"Cleaning up temp file: {tmp_file_path}")
        os.remove(tmp_file_path)