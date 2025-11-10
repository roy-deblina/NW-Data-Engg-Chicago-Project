import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import io

print("Starting load pipeline for Fact COVID Daily...")

# 1. CONFIGURATION
SILVER_CONTAINER = "silver"
SILVER_BLOB_NAME = "clean_covid_cases.parquet"
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
df['week_start'] = pd.to_datetime(df['week_start'])
df['report_date_key'] = df['week_start'].dt.strftime('%Y%m%d').astype(int)

# Create a unique primary key
df['covid_report_key'] = df['zip_code'] + '_' + df['report_date_key'].astype(str)

# Rename columns to match the database
df.rename(columns={
    'cases_weekly': 'cases_daily', # Note: Your table is 'daily' but source is 'weekly'
    'cases_cumulative': 'cases_total'
}, inplace=True)

# Select only the columns that match your fact_covid_daily table
final_columns = [
    'covid_report_key',
    'report_date_key',
    'geography_key',
    'cases_daily',
    'cases_total'
]
final_df = df.reindex(columns=final_columns)
print("...Data prepared.")

# 4. LOAD TO GOLD
try:
    print(f"Loading data into {pg_db}.fact_covid_daily...")
    final_df.to_sql(
        'fact_covid_daily',
        db_engine,
        if_exists='replace', 
        index=False,
        method='multi',
        chunksize=10000
    )
    print("...Load complete.")
    print("Load pipeline for Fact COVID Daily finished successfully.")
        
except Exception as e:
    print(f"ERROR: Failed to load to Gold: {e}")
    exit(1)