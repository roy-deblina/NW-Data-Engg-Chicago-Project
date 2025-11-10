import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import io

print("Starting load pipeline for Fact Health/Socioeconomic...")

# 1. CONFIGURATION 
SILVER_CONTAINER = "silver"
HEALTH_BLOB = "clean_public_health.parquet"
CCVI_BLOB = "clean_ccvi.parquet"
connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')

# Postgres details
pg_host = os.environ.get('PG_HOST')
pg_user = os.environ.get('PG_USER')
pg_password = os.environ.get('PG_PASSWORD')
pg_db = "chicago_bi"
db_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_db}"
db_engine = create_engine(db_url)

# 2. DOWNLOAD DATA FROM SILVER 
try:
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    
    # Download Public Health data
    print(f"Downloading {HEALTH_BLOB} from Silver...")
    health_blob_client = blob_service_client.get_blob_client(container=SILVER_CONTAINER, blob=HEALTH_BLOB)
    health_data_bytes = health_blob_client.download_blob().readall()
    df_health = pd.read_parquet(io.BytesIO(health_data_bytes))
    print(f"...Downloaded {len(df_health)} Health rows.")

    # Download CCVI data
    print(f"Downloading {CCVI_BLOB} from Silver...")
    ccvi_blob_client = blob_service_client.get_blob_client(container=SILVER_CONTAINER, blob=CCVI_BLOB)
    ccvi_data_bytes = ccvi_blob_client.download_blob().readall()
    df_ccvi = pd.read_parquet(io.BytesIO(ccvi_data_bytes))
    print(f"...Downloaded {len(df_ccvi)} CCVI rows.")

except Exception as e:
    print(f"ERROR: Failed to download from Silver: {e}")
    exit(1)

# 3. PREPARE & MERGE
print("Preparing and merging data...")

# Prepare health data
df_health['report_year'] = pd.to_numeric(df_health.get('year'), errors='coerce')
df_health.rename(columns={
    'unemployment': 'unemployment_rate',
    'below_poverty_level': 'poverty_rate'
}, inplace=True)
health_cols = ['geography_key', 'report_year', 'unemployment_rate', 'poverty_rate']
df_health = df_health.reindex(columns=health_cols)

# Prepare CCVI data
df_ccvi.rename(columns={
    'ccvi_score': 'ccvi_score', # Renaming 'ccvi_score_value' from transform script
    'ccvi_category': 'ccvi_category' # Renaming 'ccvi_category_name' from transform script
}, inplace=True)
ccvi_cols = ['geography_key', 'ccvi_score', 'ccvi_category']
df_ccvi = df_ccvi.reindex(columns=ccvi_cols)

# Join the two datasets on 'geography_key'
# We use an 'outer' join in case some community areas are in one but not the other
df_final = pd.merge(df_health, df_ccvi, on='geography_key', how='outer')

# Create a unique primary key
df_final['health_report_key'] = df_final['geography_key'].astype(str) + '_' + df_final['report_year'].astype(str)
print("...Data prepared.")

# 4. LOAD TO GOLD
try:
    print(f"Loading data into {pg_db}.fact_health_socioeconomic...")
    # Drop rows where the primary key is null (if any)
    df_final.dropna(subset=['health_report_key'], inplace=True)
    
    df_final.to_sql(
        'fact_health_socioeconomic',
        db_engine,
        if_exists='replace', 
        index=False,
        method='multi',
        chunksize=10000
    )
    print("...Load complete.")
    print("Load pipeline for Fact Health/Socioeconomic finished successfully.")
        
except Exception as e:
    print(f"ERROR: Failed to load to Gold: {e}")
    exit(1)