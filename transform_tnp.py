import os
import pandas as pd
import ijson
from sqlalchemy import create_engine, text
import io
import tempfile
import sys
import subprocess
import logging
import traceback
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("tnp_transform")

logger.info("Starting BATCH transformation pipeline for TNP Trips...")

BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"
BRONZE_BLOB_NAME = "tnp_trips_sample.json"
SILVER_BLOB_PREFIX = "clean_tnp_trips_part_"
CHUNK_SIZE = 500

AZURE_CONN_STR = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
def parse_sas_from_conn_str(conn_str):
    for part in conn_str.split(';'):
        if part.startswith('SharedAccessSignature='):
            return part.replace('SharedAccessSignature=', '')
    logger.critical("SharedAccessSignature not found in connection string")
    sys.exit(1)

try:
    SAS_TOKEN = parse_sas_from_conn_str(AZURE_CONN_STR)
    BLOB_BASE_URL = "https://chicagobistorage.blob.core.windows.net"
    BRONZE_BLOB_URL = f"{BLOB_BASE_URL}/{BRONZE_CONTAINER}/{BRONZE_BLOB_NAME}?{SAS_TOKEN}"
except Exception as ex:
    logger.critical("Failed to parse SAS from connection string", exc_info=True)
    sys.exit(1)

pg_host = os.environ.get('PG_HOST')
pg_user = os.environ.get('PG_USER')
pg_password = os.environ.get('PG_PASSWORD')
pg_db = "chicago_bi"
db_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_db}"
db_engine = create_engine(db_url)

def clean_and_join_chunk(df):
    try:
        logger.info(f"   ...Transforming {len(df)} rows...")
        df['trip_start_timestamp'] = pd.to_datetime(df.get('trip_start_timestamp'), errors='coerce')
        df['pickup_latitude'] = pd.to_numeric(df.get('pickup_centroid_latitude'), errors='coerce')
        df['pickup_longitude'] = pd.to_numeric(df.get('pickup_centroid_longitude'), errors='coerce')
        df['dropoff_latitude'] = pd.to_numeric(df.get('dropoff_centroid_latitude'), errors='coerce')
        df['dropoff_longitude'] = pd.to_numeric(df.get('dropoff_centroid_longitude'), errors='coerce')
        df.dropna(subset=['trip_id', 'trip_start_timestamp', 'pickup_latitude', 'pickup_longitude'], inplace=True)
        if df.empty:
            logger.warning("   ...Chunk is empty after cleaning.")
            return None

        locations_df = df[['pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude']].copy()
        locations_df = locations_df.reset_index().rename(columns={'index': 'temp_trip_id'})
        
        logger.info("   ...Uploading locations to temp table in DB...")
        start_time = time.time()
        with db_engine.connect() as conn:
            temp_table_name = f'temp_tnp_trip_locations_{int(time.time_ns())}'
            locations_df.to_sql(temp_table_name, conn, if_exists='replace', index=False, chunksize=10000)
        logger.info(f"   ...Uploaded to temp table in {time.time() - start_time:.2f} seconds.")
        
        logger.info("   ...Running spatial join query for pickup and dropoff...")
        start_time = time.time()
        with db_engine.connect() as conn:
            spatial_join_query = f"""
                SELECT t.temp_trip_id,
                    pickupgeo.geography_key AS pickup_geography_key,
                    dropoffgeo.geography_key AS dropoff_geography_key
                FROM {temp_table_name} AS t
                LEFT JOIN dim_geography AS pickupgeo ON ST_Contains(pickupgeo.geom, ST_SetSRID(ST_MakePoint(t.pickup_longitude, t.pickup_latitude), 4326))
                LEFT JOIN dim_geography AS dropoffgeo ON ST_Contains(dropoffgeo.geom, ST_SetSRID(ST_MakePoint(t.dropoff_longitude, t.dropoff_latitude), 4326));
            """
            geo_keys_df = pd.read_sql(spatial_join_query, conn)
        logger.info(f"   ...Spatial join completed in {time.time() - start_time:.2f} seconds.")

        logger.info("   ...Dropping temp table...")
        start_time = time.time()
        with db_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as cleanup_conn:
            cleanup_conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name};"))
        logger.info(f"   ...Temp table dropped in {time.time() - start_time:.2f} seconds.")

        df = df.reset_index().rename(columns={'index': 'temp_trip_id'})
        df_final = df.merge(geo_keys_df, on='temp_trip_id', how='left')
        logger.info(f"   ...Chunk transformed. Final size: {len(df_final)} rows.")
        return df_final
    except Exception as exc:
        logger.error("   ...ERROR: Spatial join/cleaning failed for chunk: %s", exc, exc_info=True)
        return None

def upload_chunk_to_silver(df_chunk, chunk_index):
    from azure.storage.blob import BlobServiceClient
    try:
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        silver_blob_name = f"{SILVER_BLOB_PREFIX}{chunk_index:04d}.parquet"
        logger.info(f"   ...Uploading {silver_blob_name} to Silver...")
        output_buffer = io.BytesIO()
        df_chunk.to_parquet(output_buffer, index=False)
        output_buffer.seek(0)
        blob_client = blob_service_client.get_blob_client(container=SILVER_CONTAINER, blob=silver_blob_name)
        blob_client.upload_blob(output_buffer, overwrite=True)
        logger.info(f"   ...Upload complete for chunk {chunk_index}.")
    except Exception as exc:
        logger.error(f"   ...ERROR: Failed to upload chunk {chunk_index} to Silver: %s", exc, exc_info=True)

tmp_file_path = None
try:
    with tempfile.NamedTemporaryFile(delete=False, mode='wb') as tmp_file:
        tmp_file_path = tmp_file.name
    logger.info(f"Using AzCopy to download blob to local file: {tmp_file_path}")
    sys.stdout.flush()
    cmd = [
        "azcopy",
        "copy",
        BRONZE_BLOB_URL,
        tmp_file_path,
        "--log-level=ERROR"
    ]
    logger.info("Invoking AzCopy: %s", ' '.join(cmd[:2]) + " <blob_url> <local_path>")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.critical("AzCopy FAILED: %s", result.stderr)
        raise Exception("AzCopy failed to download blob")
    logger.info("...Download completed via AzCopy.")
    logger.info(f"Streaming from temp file: {tmp_file_path}")
    with open(tmp_file_path, 'rb') as local_stream:
        tnp_stream = ijson.items(local_stream, 'item')
        chunk_data = []
        chunk_count = 0
        for trip_record in tnp_stream:
            chunk_data.append(trip_record)
            if len(chunk_data) >= CHUNK_SIZE:
                chunk_count += 1
                logger.info(f"Processing chunk {chunk_count}...")
                df_chunk_raw = pd.DataFrame(chunk_data)
                chunk_data = []
                df_chunk_final = clean_and_join_chunk(df_chunk_raw)
                if df_chunk_final is not None:
                    upload_chunk_to_silver(df_chunk_final, chunk_count)
        if chunk_data:
            chunk_count += 1
            logger.info(f"Processing final chunk {chunk_count}...")
            df_chunk_raw = pd.DataFrame(chunk_data)
            df_chunk_final = clean_and_join_chunk(df_chunk_raw)
            if df_chunk_final is not None:
                upload_chunk_to_silver(df_chunk_final, chunk_count)
    if chunk_count == 0:
        logger.warning("No data was found in the JSON stream.")
    logger.info(f"TNP transformation pipeline finished successfully. Processed {chunk_count} chunks.")
except Exception as e:
    logger.critical("ERROR: Failed in pipeline: %s", str(e))
    logger.critical(traceback.format_exc())
    sys.exit(1)
finally:
    if tmp_file_path and os.path.exists(tmp_file_path):
        logger.info(f"Cleaning up temp file: {tmp_file_path}")
        try:
            os.remove(tmp_file_path)
        except Exception:
            logger.warning(f"Could not remove temp file {tmp_file_path}", exc_info=True)
logger.info("TNP BATCH transformation pipeline completed.")
