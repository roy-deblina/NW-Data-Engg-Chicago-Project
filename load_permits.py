import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import io

print("Starting load pipeline for Fact Permits...")

# 1. CONFIGURATION
SILVER_CONTAINER = "silver"
SILVER_BLOB_NAME = "clean_building_permits.parquet"
connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')

# Postgres details
pg_host = os.environ.get('PG_HOST')
pg_user = os.environ.get('PG_USER')
pg_password = os.environ.get('PG_PASSWORD')
pg_db = "chicago_bi"
db_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_db}"
db_engine = create_engine(db_url)

# 2. DOWNLOAD FROM SILVER
try:
    print(f"Downloading {SILVER_BLOB_NAME} from Silver...")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=SILVER_CONTAINER, blob=SILVER_BLOB_NAME)
    
    downloader = blob_client.download_blob()
    clean_data_bytes = downloader.readall()
    df = pd.read_parquet(io.BytesIO(clean_data_bytes))
    print("...Download complete.")

except Exception as e:
    print(f"ERROR: Failed to download from Silver: {e}")
    exit(1)

# 3. PREPARE FOR GOLD 
print("Preparing data for Gold Zone...")
# Create date key
df['issue_date'] = pd.to_datetime(df['issue_date'])
df['issue_date_key'] = df['issue_date'].dt.strftime('%Y%m%d').astype(int)

# Rename columns to match the database
df.rename(columns={
    'id': 'permit_key',
    'permit_type': 'permit_type',
    'work_description': 'work_description',
    'reported_cost': 'reported_cost'
}, inplace=True)

# Select only the columns that match your fact_permits table
final_columns = [
    'permit_key',
    'issue_date_key',
    'geography_key',
    'permit_type',
    'work_description',
    'reported_cost'
]
final_df = df.reindex(columns=final_columns)
print("...Data prepared.")

# 4. LOAD TO GOLD
try:
    print(f"Loading data into {pg_db}.fact_permits...")
    final_df.to_sql(
        'fact_permits',
        db_engine,
        if_exists='replace', 
        index=False,
        method='multi',
        chunksize=10000
    )
    print("...Load complete.")
    print("Load pipeline for Fact Permits finished successfully.")
        
except Exception as e:
    print(f"ERROR: Failed to load to Gold: {e}")
    exit(1)