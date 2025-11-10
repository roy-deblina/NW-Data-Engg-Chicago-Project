import os
import json
from sodapy import Socrata
from azure.storage.blob import BlobServiceClient

from utils import get_logger, retry

logger = get_logger("extract_ccvi")


# Configuration
SODA_DOMAIN = "data.cityofchicago.org"
SODA_DATASET_ID = "xhc6-88s9"
BLOB_FILE_NAME = "ccvi.json"
CONTAINER_NAME = os.getenv("AZURE_BLOB_CONTAINER", "bronze")
CONNECT_STR = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")


def validate_config():
    if not CONNECT_STR:
        logger.error("Missing 'AZURE_STORAGE_CONNECTION_STRING' environment variable")
        raise SystemExit(1)


@retry(tries=3, delay=1.0, backoff=2.0)
def download_ccvi():
    logger.info("Connecting to SODA: %s", SODA_DOMAIN)
    client = Socrata(SODA_DOMAIN, None)
    logger.info("Downloading dataset %s", SODA_DATASET_ID)
    return client.get_all(SODA_DATASET_ID)


def upload_to_azure(json_data: str):
    logger.info("Connecting to Azure Blob Storage container '%s'", CONTAINER_NAME)
    blob_service_client = BlobServiceClient.from_connection_string(CONNECT_STR)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_FILE_NAME)
    logger.info("Uploading %s", BLOB_FILE_NAME)
    blob_client.upload_blob(json_data, overwrite=True)


def main():
    logger.info("Starting CCVI extraction")
    validate_config()
    try:
        results = download_ccvi()
        json_data = json.dumps(results, indent=2)
        upload_to_azure(json_data)
        logger.info("CCVI extraction finished successfully")
    except Exception as exc:
        logger.exception("CCVI extraction failed: %s", exc)
        raise SystemExit(1)


if __name__ == "__main__":
    main()