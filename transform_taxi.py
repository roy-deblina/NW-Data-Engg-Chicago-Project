import os
import json
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import io # Needed to read/write from memory

print("Starting transformation pipeline for Taxi Trips...")

# 1. CONFIGURATION 
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"
# We now look for a prefix, not a single file name
BRONZE_BLOB_PREFIX = "taxi_trips_part_" 
SILVER_BLOB_NAME = "clean_taxi_trips.parquet"
connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')

# Postgres details
pg_host = os.environ.get('PG_HOST')
pg_user = os.environ.get('PG_USER')
pg_password = os.environ.get('PG_PASSWORD')
pg_db = "chicago_bi"
db_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_db}"
db_engine = create_engine(db_url)

# 2. DOWNLOAD AND COMBINE ALL CHUNKS FROM BRONZE 
try:
    print(f"Connecting to Azure container '{BRONZE_CONTAINER}'...")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(BRONZE_CONTAINER)

    all_dfs = [] # A list to hold all our DataFrames
    
    print(f"Finding and downloading all blobs with prefix '{BRONZE_BLOB_PREFIX}'...")
    blob_list = container_client.list_blobs(name_starts_with=BRONZE_BLOB_PREFIX)
    
    for blob in blob_list:
        print(f"...Downloading {blob.name}")
        blob_client = blob_service_client.get_blob_client(container=BRONZE_CONTAINER, blob=blob.name)
        
        downloader = blob_client.download_blob()
        raw_data = json.loads(downloader.readall())
        temp_df = pd.DataFrame(raw_data)
        all_dfs.append(temp_df)
        print(f"   ...Downloaded {len(temp_df)} rows.")

    if not all_dfs:
        print("ERROR: No part files found. Exiting.")
        exit(1)

    # Combine all the individual DataFrames into one master DataFrame
    print(f"Combining {len(all_dfs)} files into one master DataFrame...")
    df = pd.concat(all_dfs, ignore_index=True)
    print(f"...Downloaded a total of {len(df)} rows.")

except Exception as e:
    print(f"ERROR: Failed to download from Bronze: {e}")
    exit(1)

# 3. CLEAN & PREPARE DATA
print("Cleaning and preparing data...")
# Convert types, turning errors into 'NaN'
df['trip_start_timestamp'] = pd.to_datetime(df.get('trip_start_timestamp'), errors='coerce')
df['pickup_latitude'] = pd.to_numeric(df.get('pickup_latitude'), errors='coerce')
df['pickup_longitude'] = pd.to_numeric(df.get('pickup_longitude'), errors='coerce')
df['dropoff_latitude'] = pd.to_numeric(df.get('dropoff_latitude'), errors='coerce')
df['dropoff_longitude'] = pd.to_numeric(df.get('dropoff_longitude'), errors='coerce')

# Drop any rows where essential data is missing
df.dropna(subset=['trip_id', 'trip_start_timestamp', 'pickup_latitude', 'pickup_longitude'], inplace=True)

# Create a clean DataFrame of just the locations we need to look up
# We add a unique index to join on later
locations_df = df[['pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude']].copy()
locations_df = locations_df.reset_index().rename(columns={'index': 'temp_trip_id'})
print(f"Cleaned data, {len(locations_df)} rows to geocode.")


# 4. PERFORM SPATIAL JOIN (The Fast Way) 
print("Performing high-speed spatial join...")
try:
    with db_engine.connect() as conn:
        # Upload locations to a temporary table
        print("Uploading locations to temp table...")
        # Use chunksize for better performance with large data
        locations_df.to_sql('temp_trip_locations', conn, if_exists='replace', index=False, chunksize=10000)
        print("...Upload complete.")

        # Run ONE single query to join everything using PostGIS
        print("Running SQL spatial join...")
        spatial_join_query = """
            SELECT
                t.temp_trip_id,
                pickup_geo.geography_key AS pickup_geography_key,
                dropoff_geo.geography_key AS dropoff_geography_key
            FROM
                temp_trip_locations AS t
            LEFT JOIN
                dim_geography AS pickup_geo ON ST_Contains(pickup_geo.geom, ST_SetSRID(ST_MakePoint(t.pickup_longitude, t.pickup_latitude), 4326))
            LEFT JOIN
                dim_geography AS dropoff_geo ON ST_Contains(dropoff_geo.geom, ST_SetSRID(ST_MakePoint(t.dropoff_longitude, t.dropoff_latitude), 4326));
        """
        
        # Get the results back into a new DataFrame
        geo_keys_df = pd.read_sql(spatial_join_query, conn)
        
        # Clean up the temp table
        conn.execute("DROP TABLE IF EXISTS temp_trip_locations;")
        print("...Spatial join complete.")

except Exception as e:
    print(f"ERROR: Spatial join failed: {e}")
    exit(1)

# 5. MERGE & FINALIZE 
print("Merging geography keys back into main dataset...")
# Add the temp_trip_id to the original dataframe to join
df = df.reset_index().rename(columns={'index': 'temp_trip_id'})
# Merge the geography keys back
df_final = df.merge(geo_keys_df, on='temp_trip_id', how='left')
print("...Merge complete.")

# 6. UPLOAD TO SILVER
try:
    print(f"Uploading {SILVER_BLOB_NAME} to Silver...")
    # Convert DataFrame to Parquet format in memory
    output_buffer = io.BytesIO()
    df_final.to_parquet(output_buffer, index=False)
    output_buffer.seek(0)
    
    # Upload to Silver
    blob_client = blob_service_client.get_blob_client(container=SILVER_CONTAINER, blob=SILVER_BLOB_NAME)
    blob_client.upload_blob(output_buffer, overwrite=True)
    
    print("...Upload complete.")
    print("Transformation pipeline finished successfully.")
    
except Exception as e:
    print(f"ERROR: Failed to upload to Silver: {e}")
    exit(1)