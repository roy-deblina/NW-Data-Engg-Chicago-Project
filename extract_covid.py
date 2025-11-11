import os
import json
from sodapy import Socrata
from azure.storage.blob import BlobServiceClient

from utils import get_logger, retry

logger = get_logger("extract_covid")

SODA_DOMAIN = "data.cityofchicago.org"
SODA_DATASET_ID = "yhhz-zm2v"
BLOB_FILE_NAME = "covid_cases.json"
CONTAINER_NAME = os.getenv("AZURE_BLOB_CONTAINER", "bronze")
CONNECT_STR = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")


def validate_config():
    if not CONNECT_STR:
        logger.error("Missing 'AZURE_STORAGE_CONNECTION_STRING' environment variable")
        raise SystemExit(1)


@retry(tries=3, delay=1.0, backoff=2.0)
def download_data():
    logger.info("Downloading dataset %s from %s", SODA_DATASET_ID, SODA_DOMAIN)
    client = Socrata(SODA_DOMAIN, None)
    return list(client.get_all(SODA_DATASET_ID))


def upload_to_azure(json_data: str):
    logger.info("Uploading %s to Azure container %s", BLOB_FILE_NAME, CONTAINER_NAME)
    blob_service_client = BlobServiceClient.from_connection_string(CONNECT_STR)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_FILE_NAME)
    blob_client.upload_blob(json_data, overwrite=True)


def main():
    logger.info("Starting COVID extract")
    validate_config()
    try:
        results = download_data()
        json_data = json.dumps(results, indent=2)
        upload_to_azure(json_data)
        logger.info("COVID extract finished successfully")
    except Exception as exc:
        logger.exception("COVID extract failed: %s", exc)
        raise SystemExit(1)


if __name__ == "__main__":
    main()