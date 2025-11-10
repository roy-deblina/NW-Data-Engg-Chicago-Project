import os
import json
from sodapy import Socrata
from azure.storage.blob import BlobServiceClient

from utils import get_logger, retry

logger = get_logger("extract_taxi")

SODA_DOMAIN = "data.cityofchicago.org"
SODA_DATASET_ID = "wrvz-psew"
DATASET_LIMIT = int(os.getenv("DATASET_LIMIT", "1000000"))
BLOB_FILE_NAME = "taxi_trips_sample.json"
CONTAINER_NAME = os.getenv("AZURE_BLOB_CONTAINER", "bronze")
CONNECT_STR = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")


def validate_config():
    if not CONNECT_STR:
        logger.error("Missing 'AZURE_STORAGE_CONNECTION_STRING' environment variable")
        raise SystemExit(1)


@retry(tries=3, delay=1.0, backoff=2.0)
def download_data(limit: int):
    logger.info("Downloading %s rows from dataset %s", limit, SODA_DATASET_ID)
    client = Socrata(SODA_DOMAIN, None)
    return client.get(SODA_DATASET_ID, limit=limit)


def upload_to_azure(json_data: str):
    logger.info("Uploading %s to Azure container %s", BLOB_FILE_NAME, CONTAINER_NAME)
    blob_service_client = BlobServiceClient.from_connection_string(CONNECT_STR)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_FILE_NAME)
    blob_client.upload_blob(json_data, overwrite=True)


def main():
    logger.info("Starting Taxi Trips extract")
    validate_config()
    try:
        results = download_data(DATASET_LIMIT)
        json_data = json.dumps(results, indent=2)
        upload_to_azure(json_data)
        logger.info("Taxi Trips extract finished successfully")
    except Exception as exc:
        logger.exception("Taxi Trips extract failed: %s", exc)
        raise SystemExit(1)


if __name__ == "__main__":
    main()