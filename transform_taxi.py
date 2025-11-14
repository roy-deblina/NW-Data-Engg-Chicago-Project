import os
import sys
import subprocess
import tempfile
import json
import ijson
import pandas as pd
from sqlalchemy import create_engine, text
import io
import time
import logging
import traceback

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("taxi_transform")

logger.info("Starting BATCH transformation pipeline for Taxi Trips...")

# Configuration
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"
BRONZE_BLOB_PREFIX = "taxi_trips_part_"  # Adjust as needed for your prefix
SILVER_BLOB_PREFIX = "clean_taxi_trips/part_"
connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
CHUNK_SIZE = 1000  # For RAM safety and load balance

# Postgres details
pg_host = os.environ.get('PG_HOST')
pg_user = os.environ.get('PG_USER')
pg_password = os.environ.get('PG_PASSWORD')
pg_db = "chicago_bi"
db_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}/{pg_db}"
db_engine = create_engine(db_url)

# Helper: Extract SAS from connection string
def parse_sas_from_conn_str(conn_str):
    for part in conn_str.split(';'):
        if part.startswith('SharedAccessSignature='):
            return part.replace('SharedAccessSignature=', '')
    logger.error("SharedAccessSignature not found in connection string")
    sys.exit(1)

# Helper: Clean, join, spatial logic
import time
from sqlalchemy import text

# def clean_and_join_chunk(chunk_df, db_engine, logger):
#     start_total = time.time()
#     try:
#         # Step 1: Data Cleaning/Preparation
#         t0 = time.time()
#         logger.info(f"   ...Transforming {len(chunk_df)} rows...")

#         df = chunk_df.copy()
#         df['trip_start_timestamp'] = pd.to_datetime(df.get('trip_start_timestamp'), errors='coerce')
#         df['pickup_latitude'] = pd.to_numeric(df.get('pickup_centroid_latitude'), errors='coerce')
#         df['pickup_longitude'] = pd.to_numeric(df.get('pickup_centroid_longitude'), errors='coerce')
#         df['dropoff_latitude'] = pd.to_numeric(df.get('dropoff_centroid_latitude'), errors='coerce')
#         df['dropoff_longitude'] = pd.to_numeric(df.get('dropoff_centroid_longitude'), errors='coerce')
#         df.dropna(subset=['trip_id', 'trip_start_timestamp', 'pickup_latitude', 'pickup_longitude'], inplace=True)
#         if df.empty:
#             logger.warning("   ...Chunk is empty after cleaning.")
#             logger.info(f"   Data clean step time: {time.time() - t0:.2f}s")
#             return None
#         logger.info(f"   Data clean step time: {time.time() - t0:.2f}s")

#         # Step 2: Prepare locations DataFrame
#         t1 = time.time()
#         locations_df = df[['pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude']].reset_index().rename(columns={'index': 'temp_trip_id'})
#         logger.info(f"   Locations prep step time: {time.time() - t1:.2f}s")

#         # Step 3: Write/Truncate to persistent temp table
#         t2 = time.time()
#         with db_engine.begin() as conn:  # Use begin to allow truncate and insert in one transaction
#             conn.execute(text("TRUNCATE temp_taxi_trip_locations;"))
#             locations_df.to_sql('temp_taxi_trip_locations', conn, if_exists="append", index=False, method='multi')
#         logger.info(f"   Temp table write step time: {time.time() - t2:.2f}s")

#         # Step 4: Spatial join with bounding box filter (PostGIS)
#         t3 = time.time()
#         with db_engine.connect() as conn:
#             spatial_join_query = """
#                 SELECT t.temp_trip_id,
#                     pickupgeo.geography_key AS pickup_geography_key,
#                     dropoffgeo.geography_key AS dropoff_geography_key
#                 FROM temp_taxi_trip_locations t
#                 LEFT JOIN dim_geography pickupgeo
#                     ON pickupgeo.geom && ST_MakeEnvelope(t.pickup_longitude-0.01, t.pickup_latitude-0.01, t.pickup_longitude+0.01, t.pickup_latitude+0.01, 4326)
#                     AND ST_Contains(pickupgeo.geom, ST_SetSRID(ST_MakePoint(t.pickup_longitude, t.pickup_latitude), 4326))
#                 LEFT JOIN dim_geography dropoffgeo
#                     ON dropoffgeo.geom && ST_MakeEnvelope(t.dropoff_longitude-0.01, t.dropoff_latitude-0.01, t.dropoff_longitude+0.01, t.dropoff_latitude+0.01, 4326)
#                     AND ST_Contains(dropoffgeo.geom, ST_SetSRID(ST_MakePoint(t.dropoff_longitude, t.dropoff_latitude), 4326));
#             """
#             geo_keys_df = pd.read_sql(spatial_join_query, conn)
#         logger.info(f"   Spatial join step time: {time.time() - t3:.2f}s")

#         # Step 5: DataFrame Merge
#         t4 = time.time()
#         df = df.reset_index().rename(columns={'index': 'temp_trip_id'})
#         df_final = df.merge(geo_keys_df, on='temp_trip_id', how='left')
#         logger.info(f"   Merge step time: {time.time() - t4:.2f}s")

#         total_elapsed = time.time() - start_total
#         logger.info(f"   ...Chunk transformed. Final size: {len(df_final)} rows. Total chunk time: {total_elapsed:.2f}s")
#         return df_final

#     except Exception as exc:
#         logger.error("   ...ERROR: Spatial join/cleaning failed for chunk: %s", exc, exc_info=True)
#         return None

def clean_and_join_chunk(chunk_df, db_engine, logger):
    start_total = time.time()
    try:
        # Step 1: Data Cleaning/Preparation
        t0 = time.time()
        logger.info(f"   ...Transforming {len(chunk_df)} rows...")
        df = chunk_df.copy()
        df['trip_start_timestamp'] = pd.to_datetime(df.get('trip_start_timestamp'), errors='coerce')
        df['pickup_latitude'] = pd.to_numeric(df.get('pickup_centroid_latitude'), errors='coerce')
        df['pickup_longitude'] = pd.to_numeric(df.get('pickup_centroid_longitude'), errors='coerce')
        df['dropoff_latitude'] = pd.to_numeric(df.get('dropoff_centroid_latitude'), errors='coerce')
        df['dropoff_longitude'] = pd.to_numeric(df.get('dropoff_centroid_longitude'), errors='coerce')
        df.dropna(subset=['trip_id', 'trip_start_timestamp', 'pickup_latitude', 'pickup_longitude'], inplace=True)
        if df.empty:
            logger.warning("   ...Chunk is empty after cleaning.")
            logger.info(f"   Data clean step time: {time.time() - t0:.2f}s")
            return None
        logger.info(f"   Data clean step time: {time.time() - t0:.2f}s")

        # Step 2: Prepare locations DataFrame
        t1 = time.time()
        locations_df = df[['pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude']].reset_index().rename(columns={'index': 'temp_trip_id'})
        logger.info(f"   Locations prep step time: {time.time() - t1:.2f}s")

        # Step 3: Create and Write to temp table (within same session)
        t2 = time.time()
        with db_engine.begin() as conn:
            conn.execute(text("""
                CREATE TEMP TABLE IF NOT EXISTS temp_taxi_trip_locations (
                    temp_trip_id INTEGER,
                    pickup_latitude DOUBLE PRECISION,
                    pickup_longitude DOUBLE PRECISION,
                    dropoff_latitude DOUBLE PRECISION,
                    dropoff_longitude DOUBLE PRECISION
                ) ON COMMIT PRESERVE ROWS;
            """))
            conn.execute(text("TRUNCATE temp_taxi_trip_locations;"))
            locations_df.to_sql('temp_taxi_trip_locations', conn, if_exists="append", index=False, method='multi')
        logger.info(f"   Temp table write step time: {time.time() - t2:.2f}s")

        # Step 4: Spatial join with bounding box optimization
        t3 = time.time()
        with db_engine.connect() as conn:
            spatial_join_query = """
                SELECT t.temp_trip_id,
                    pickupgeo.geography_key AS pickup_geography_key,
                    dropoffgeo.geography_key AS dropoff_geography_key
                FROM temp_taxi_trip_locations t
                LEFT JOIN dim_geography pickupgeo
                    ON pickupgeo.geom && ST_MakeEnvelope(t.pickup_longitude-0.01, t.pickup_latitude-0.01, t.pickup_longitude+0.01, t.pickup_latitude+0.01, 4326)
                    AND ST_Contains(pickupgeo.geom, ST_SetSRID(ST_MakePoint(t.pickup_longitude, t.pickup_latitude), 4326))
                LEFT JOIN dim_geography dropoffgeo
                    ON dropoffgeo.geom && ST_MakeEnvelope(t.dropoff_longitude-0.01, t.dropoff_latitude-0.01, t.dropoff_longitude+0.01, t.dropoff_latitude+0.01, 4326)
                    AND ST_Contains(dropoffgeo.geom, ST_SetSRID(ST_MakePoint(t.dropoff_longitude, t.dropoff_latitude), 4326));
            """
            geo_keys_df = pd.read_sql(spatial_join_query, conn)
        logger.info(f"   Spatial join step time: {time.time() - t3:.2f}s")

        # Step 5: DataFrame Merge
        t4 = time.time()
        df = df.reset_index().rename(columns={'index': 'temp_trip_id'})
        df_final = df.merge(geo_keys_df, on='temp_trip_id', how='left')
        logger.info(f"   Merge step time: {time.time() - t4:.2f}s")

        total_elapsed = time.time() - start_total
        logger.info(f"   ...Chunk transformed. Final size: {len(df_final)} rows. Total chunk time: {total_elapsed:.2f}s")
        return df_final

    except Exception as exc:
        logger.error("   ...ERROR: Spatial join/cleaning failed for chunk: %s", exc, exc_info=True)
        return None


# Helper: Upload chunk to Azure
def upload_chunk_to_silver(df_chunk, chunk_index):
    from azure.storage.blob import BlobServiceClient
    try:
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

try:
    logger.info(f"Connecting to Azure container '{BRONZE_CONTAINER}'...")
    SAS_TOKEN = parse_sas_from_conn_str(connect_str)
    BLOB_BASE_URL = "https://chicagobistorage.blob.core.windows.net"
    from azure.storage.blob import ContainerClient
    container_client = ContainerClient(account_url=f"{BLOB_BASE_URL}", container_name=BRONZE_CONTAINER, credential=SAS_TOKEN)
    blobs_to_process = [blob for blob in container_client.list_blobs(name_starts_with=BRONZE_BLOB_PREFIX) if blob.name.endswith('.json')]
    if not blobs_to_process:
        logger.error("No part files found. Exiting.")
        sys.exit(1)
    logger.info(f"Found {len(blobs_to_process)} part files to process.")

    chunk_index = 0
    for blob in blobs_to_process:
        logger.info(f"Processing file: {blob.name}")

        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_path = tmp_file.name

        # Use azcopy to download fast
        blob_url = f"{BLOB_BASE_URL}/{BRONZE_CONTAINER}/{blob.name}?{SAS_TOKEN}"
        cmd = ["azcopy", "copy", blob_url, tmp_path, "--log-level=ERROR"]
        logger.info("Invoking AzCopy: %s", ' '.join(cmd[:2]) + " <blob_url> <local_path>")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error("AzCopy FAILED: %s", result.stderr)
            os.remove(tmp_path)
            continue
        logger.info(f"...Download completed via AzCopy to {tmp_path}.")

        # Streaming parse with ijson
        with open(tmp_path, 'rb') as local_stream:
            taxi_stream = ijson.items(local_stream, 'item')
            row_buffer = []
            for trip_record in taxi_stream:
                row_buffer.append(trip_record)
                if len(row_buffer) >= CHUNK_SIZE:
                    chunk_index += 1
                    logger.info(f"Processing chunk {chunk_index} in {blob.name}")
                    df_chunk_raw = pd.DataFrame(row_buffer)
                    row_buffer = []
                    df_chunk_final = clean_and_join_chunk(df_chunk_raw, db_engine, logger)
                    if df_chunk_final is not None:
                        upload_chunk_to_silver(df_chunk_final, chunk_index)
            if row_buffer:
                chunk_index += 1
                logger.info(f"Processing tail chunk {chunk_index} (last rows) in {blob.name}")
                df_chunk_raw = pd.DataFrame(row_buffer)
                df_chunk_final = clean_and_join_chunk(df_chunk_raw, db_engine, logger)
                if df_chunk_final is not None:
                    upload_chunk_to_silver(df_chunk_final, chunk_index)
        logger.info(f"Done processing file: {blob.name}")
        try:
            os.remove(tmp_path)
        except Exception:
            logger.warning(f"Could not remove temp file {tmp_path}", exc_info=True)

    logger.info(f"Taxi transformation pipeline finished successfully. Total chunks: {chunk_index}")

except Exception as e:
    logger.critical("ERROR: Failed during batch transform process: %s", e)
    logger.critical(traceback.format_exc())
    sys.exit(1)
