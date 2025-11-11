import os
import json
from sodapy import Socrata
from azure.storage.blob import BlobServiceClient, ContainerClient
import time
import re

from utils import get_logger

logger = get_logger("extract_taxi")

SODA_DOMAIN = "data.cityofchicago.org"
SODA_DATASET_ID = "wrvz-psew"
DATASET_LIMIT = int(os.getenv("DATASET_LIMIT", "1000000"))
CHUNK_SIZE = int(os.getenv("SODA_CHUNK_SIZE", "50000"))
BLOB_FILE_PREFIX = "taxi_trips_part"
CONTAINER_NAME = os.getenv("AZURE_BLOB_CONTAINER", "bronze")
CONNECT_STR = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
SODA_APP_TOKEN = os.getenv("SODA_APP_TOKEN", "3LwO06uCCI9Plm1P7gNAi6Tjv")


def validate_config():
    if not CONNECT_STR:
        logger.error("Missing 'AZURE_STORAGE_CONNECTION_STRING' environment variable")
        raise SystemExit(1)
    
    if not SODA_APP_TOKEN:
        logger.warning("Missing 'SODA_APP_TOKEN'. Requests will be severely throttled.")


def get_resume_checkpoint(container_client: ContainerClient) -> (int, int):
    """
    Checks the container for existing part files to determine where to resume.
    Returns (next_offset, next_file_index)
    """
    logger.info("Checking for existing files to resume download...")
    blobs = container_client.list_blobs(name_starts_with=BLOB_FILE_PREFIX)
    
    max_part_num = 0
    file_pattern = re.compile(f"{BLOB_FILE_PREFIX}_(\\d{{3}})\\.json")
    
    try:
        for blob in blobs:
            match = file_pattern.match(blob.name)
            if match:
                part_num = int(match.group(1))
                if part_num > max_part_num:
                    max_part_num = part_num
    except Exception as e:
        logger.warning(f"Error checking blobs, defaulting to start from 0: {e}")
        return 0, 1

    if max_part_num > 0:
        next_file_index = max_part_num + 1
        next_offset = max_part_num * CHUNK_SIZE
        logger.info(f"Resuming download. Found part {max_part_num}. Starting from offset {next_offset}.")
        return next_offset, next_file_index
    else:
        logger.info("No existing parts found. Starting from offset 0.")
        return 0, 1


def download_chunk(client, offset, limit):
    """
    Downloads a single chunk of data from the Socrata API with retries.
    """
    max_retries = 5
    for attempt in range(max_retries):
        try:
            logger.info("Requesting offset=%s limit=%s (Attempt %s/%s)", offset, limit, attempt + 1, max_retries)
            page = client.get(SODA_DATASET_ID, limit=limit, offset=offset)
            return page  # Success
        except Exception as e:
            logger.warning("Chunk download failed (offset=%s): %s. Retrying in 15s...", offset, e)
            if attempt + 1 == max_retries:
                logger.error("Chunk failed after %s retries. Aborting.", max_retries)
                raise  # Re-raise the exception to fail the script
            time.sleep(15)  # Wait 15 seconds before retrying
    return None


def upload_chunk_to_azure(blob_service_client, json_data_str, file_name):
    """
    Uploads a string of JSON data as a new blob.
    """
    logger.info("Uploading chunk to %s/%s", CONTAINER_NAME, file_name)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=file_name)
    blob_client.upload_blob(json_data_str, overwrite=True)


def main():
    logger.info("Starting Taxi Trips extract (Production, Resumable Strategy)")
    validate_config()
    
    socrata_client = Socrata(SODA_DOMAIN, SODA_APP_TOKEN, timeout=600)
    blob_service_client = BlobServiceClient.from_connection_string(CONNECT_STR)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    
    # --- THIS IS THE NEW LOGIC ---
    start_offset, start_index = get_resume_checkpoint(container_client)
    
    offset = start_offset
    file_index = start_index
    total_downloaded = start_offset  # Start counting from where we left off
    # --- END NEW LOGIC ---
    
    try:
        while total_downloaded < DATASET_LIMIT:
            # 1. Download one chunk
            to_get = min(CHUNK_SIZE, DATASET_LIMIT - total_downloaded)
            
            chunk_data = download_chunk(socrata_client, offset, to_get)
            
            if not chunk_data:
                logger.info("No more data received from API. Ending process.")
                break
            
            num_rows_in_chunk = len(chunk_data)
            
            # 2. Convert chunk to JSON string
            json_data = json.dumps(chunk_data, indent=2)
            
            # 3. Upload this chunk to Azure as its own file
            file_name = f"{BLOB_FILE_PREFIX}_{file_index:03d}.json"
            upload_chunk_to_azure(blob_service_client, json_data, file_name)
            
            logger.info("Successfully uploaded chunk %s (%s rows)", file_index, num_rows_in_chunk)
            
            # 4. Update counters and loop
            total_downloaded += num_rows_in_chunk
            offset += num_rows_in_chunk
            file_index += 1
            
            if num_rows_in_chunk < to_get:
                logger.info("Received fewer rows than requested. Assuming end of dataset.")
                break
        
        logger.info("Taxi Trips extract finished successfully. Total rows downloaded: %s", total_downloaded)
        
    except Exception as exc:
        logger.exception("Taxi Trips extract failed: %s", exc)
        raise SystemExit(1)


if __name__ == "__main__":
    main()