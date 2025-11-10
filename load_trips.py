import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import io

print("Starting load pipeline for Fact Trips...")

# 1. CONFIGURATION 
SILVER_CONTAINER = "silver"
TAXI_BLOB_NAME = "clean_taxi_trips.parquet"
TNP_BLOB_NAME = "clean_tnp_trips.parquet"
connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')

# Postgres details
pg_host = os.environ.get('PG_HOST')
pg_user = os.environ.get('PG_USER')
pg_password = os.environ.get('PG_PASSWORD')
pg_db = "chicago_bi"
db_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_db}"
db_engine = create_engine(db_url)

# 2. DOWNLOAD & COMBINE DATA
try:
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    
    # Download Taxi data
    print(f"Downloading {TAXI_BLOB_NAME} from Silver...")
    taxi_blob_client = blob_service_client.get_blob_client(container=SILVER_CONTAINER, blob=TAXI_BLOB_NAME)
    taxi_data_bytes = taxi_blob_client.download_blob().readall()
    df_taxi = pd.read_parquet(io.BytesIO(taxi_data_bytes))
    df_taxi['trip_source'] = 'Taxi' # Add trip_source
    print(f"...Downloaded {len(df_taxi)} Taxi rows.")

    # Download TNP data
    print(f"Downloading {TNP_BLOB_NAME} from Silver...")
    tnp_blob_client = blob_service_client.get_blob_client(container=SILVER_CONTAINER, blob=TNP_BLOB_NAME)
    tnp_data_bytes = tnp_blob_client.download_blob().readall()
    df_tnp = pd.read_parquet(io.BytesIO(tnp_data_bytes))
    df_tnp['trip_source'] = 'TNP' # Add trip_source
    print(f"...Downloaded {len(df_tnp)} TNP rows.")

    # Combine them into one DataFrame
    df_combined = pd.concat([df_taxi, df_tnp], ignore_index=True)
    print(f"Total trips to load: {len(df_combined)}")

except Exception as e:
    print(f"ERROR: Failed to download from Silver: {e}")
    exit(1)

# 3. PREPARE FOR GOLD 
print("Preparing data for Gold Zone...")

# Create date keys (YYYYMMDD as an integer)
df_combined['trip_start_timestamp'] = pd.to_datetime(df_combined['trip_start_timestamp'])
df_combined['trip_start_date_key'] = df_combined['trip_start_timestamp'].dt.strftime('%Y%m%d').astype(int)
    
# Handle potential missing end times
df_combined['trip_end_timestamp'] = pd.to_datetime(df_combined.get('trip_end_timestamp'), errors='coerce')
df_combined['trip_end_date_key'] = df_combined['trip_end_timestamp'].dt.strftime('%Y%m%d')
df_combined['trip_end_date_key'].fillna(0, inplace=True) # Use 0 for missing end dates
df_combined['trip_end_date_key'] = df_combined['trip_end_date_key'].astype(int)

# Select only the columns that match your fact_trips table
final_columns = [
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
    
# Filter df to only these columns, handling missing ones
final_df = df_combined.reindex(columns=final_columns)
print("...Data prepared.")

# 4. LOAD TO GOLD 
try:
    print(f"Loading data into {pg_db}.fact_trips...")
    # 'if_exists='replace'' will wipe the table first. This is good for testing.
    final_df.to_sql(
        'fact_trips',
        db_engine,
        if_exists='replace', 
        index=False,
        method='multi', # Use 'multi' for faster inserts
        chunksize=10000  # Load 10,000 rows at a time
    )
    print("...Load complete.")
    print("Load pipeline for Fact Trips finished successfully.")
        
except Exception as e:
    print(f"ERROR: Failed to load to Gold: {e}")
    exit(1)