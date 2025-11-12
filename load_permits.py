import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import io
import logging
import sys

# Setup Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("fact_permits_load")

logger.info("Starting BATCH load pipeline for Fact Permits...")

# CONFIG
SILVER_CONTAINER = "silver"
SILVER_BLOB_PREFIX = "clean_building_permits/"
connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
DB_TABLE_NAME = "fact_permits"
pg_host = os.environ.get('PG_HOST')
pg_user = os.environ.get('PG_USER')
pg_password = os.environ.get('PG_PASSWORD')
pg_db = "chicago_bi"
db_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_db}"
db_engine = create_engine(db_url)

try:
    logger.info(f"Connecting to Azure container '{SILVER_CONTAINER}'...")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(SILVER_CONTAINER)

    logger.info(f"Finding all blobs with prefix '{SILVER_BLOB_PREFIX}'...")
    blob_list = container_client.list_blobs(name_starts_with=SILVER_BLOB_PREFIX)

    first_chunk = True
    for blob in blob_list:
        if not blob.name.endswith('.parquet'):
            continue
        try:
            logger.info(f"...Processing {blob.name}")

            blob_client = blob_service_client.get_blob_client(container=SILVER_CONTAINER, blob=blob.name)
            downloader = blob_client.download_blob()
            clean_data_bytes = downloader.readall()
            df = pd.read_parquet(io.BytesIO(clean_data_bytes))
            logger.info(f"   ...Downloaded {len(df)} rows.")

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
            final_df = df.reindex(columns=final_columns)

            load_method = 'replace' if first_chunk else 'append'
            if first_chunk:
                logger.info(f"   ...Replacing table '{DB_TABLE_NAME}' with first chunk.")
                first_chunk = False
            else:
                logger.info(f"   ...Appending chunk to table '{DB_TABLE_NAME}'.")

            final_df.to_sql(
                DB_TABLE_NAME,
                db_engine,
                if_exists=load_method,
                index=False,
                method='multi',
                chunksize=10000
            )
            logger.info("   ...Chunk loaded successfully.")
        except Exception as ex:
            logger.error(f"   ...Failed to process blob {blob.name}: {ex}", exc_info=True)

    if first_chunk:
        logger.warning("No parquet files were found. No data was loaded.")
    else:
        logger.info("Load pipeline for Fact Permits finished successfully.")

except Exception as e:
    logger.critical(f"Failed during batch load process: {e}", exc_info=True)
    sys.exit(1)

logger.info("Batch load pipeline for Fact Permits completed.")