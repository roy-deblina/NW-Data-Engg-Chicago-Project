import os
import json
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import io

print("Starting transformation pipeline for Building Permits...")

# 1. CONFIGURATION 
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"
BRONZE_BLOB_NAME = "building_permits.json" #  CHANGED
SILVER_BLOB_NAME = "clean_building_permits.parquet" #  CHANGED
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

# 3. CLEAN & PREPARE DATA 
print("Cleaning and preparing data...")
# Convert types
df['issue_date'] = pd.to_datetime(df.get('issue_date'), errors='coerce')
df['latitude'] = pd.to_numeric(df.get('latitude'), errors='coerce')
df['longitude'] = pd.to_numeric(df.get('longitude'), errors='coerce')

# Drop any rows where essential data is missing
df.dropna(subset=['id', 'issue_date', 'latitude', 'longitude'], inplace=True)

# Create a clean DataFrame of just the locations we need to look up
locations_df = df[['latitude', 'longitude']].copy()
locations_df = locations_df.reset_index().rename(columns={'index': 'temp_permit_id'})
print(f"Cleaned data, {len(locations_df)} rows to geocode.")


# 4. SPATIAL JOIN 
print("Performing high-speed spatial join...")
try:
    with db_engine.connect() as conn:
        temp_table_name = 'temp_permit_locations'
        
        # Upload locations to a temporary table
        print(f"Uploading locations to {temp_table_name}...")
        locations_df.to_sql(temp_table_name, conn, if_exists='replace', index=False)
        print("...Upload complete.")

        # Run ONE single query to join everything using PostGIS
        print("Running SQL spatial join...")
        spatial_join_query = f"""
            SELECT
                t.temp_permit_id,
                geo.geography_key
            FROM
                {temp_table_name} AS t
            LEFT JOIN
                dim_geography AS geo ON ST_Contains(geo.geom, ST_SetSRID(ST_MakePoint(t.longitude, t.latitude), 4326));
        """
        
        # Get the results back into a new DataFrame
        geo_keys_df = pd.read_sql(spatial_join_query, conn)
        
        # Clean up the temp table
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(f"DROP TABLE {temp_table_name};")
        print("...Spatial join complete.")

except Exception as e:
    print(f"ERROR: Spatial join failed: {e}")
    exit(1)

# 5. MERGE & FINALIZE
print("Merging geography keys back into main dataset...")
df = df.reset_index().rename(columns={'index': 'temp_permit_id'})
df_final = df.merge(geo_keys_df, on='temp_permit_id', how='left')
print("...Merge complete.")

# 6. UPLOAD TO SILVER 
try:
    print(f"Uploading {SILVER_BLOB_NAME} to Silver...")
    output_buffer = io.BytesIO()
    df_final.to_parquet(output_buffer, index=False)
    output_buffer.seek(0)
    
    blob_client = blob_service_client.get_blob_client(container=SILVER_CONTAINER, blob=SILVER_BLOB_NAME)
    blob_client.upload_blob(output_buffer, overwrite=True)
    
    print("...Upload complete.")
    print("Permits transformation pipeline finished successfully.")
    
except Exception as e:
    print(f"ERROR: Failed to upload to Silver: {e}")
    exit(1)
