import os
import json
from sodapy import Socrata
from azure.storage.blob import BlobServiceClient

from utils import get_logger, retry

logger = get_logger("extract_tnp")

SODA_DOMAIN = "data.cityofchicago.org"
SODA_DATASET_ID = "m6dm-c72p"
DATASET_LIMIT = int(os.getenv("DATASET_LIMIT", "1000000"))
BLOB_FILE_NAME = "tnp_trips_sample.json"
CONTAINER_NAME = os.getenv("AZURE_BLOB_CONTAINER", "bronze")
CONNECT_STR = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
import os
import json
import tempfile
from sodapy import Socrata
from azure.storage.blob import BlobServiceClient

from utils import get_logger, retry

logger = get_logger("extract_tnp")

SODA_DOMAIN = "data.cityofchicago.org"
SODA_DATASET_ID = "m6dm-c72p"
CHUNK_SIZE = int(os.getenv("SODA_CHUNK_SIZE", "50000"))
DATASET_LIMIT = int(os.getenv("DATASET_LIMIT", "1000000"))
BLOB_FILE_NAME = "tnp_trips_sample.json"
CONTAINER_NAME = os.getenv("AZURE_BLOB_CONTAINER", "bronze")
CONNECT_STR = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
SODA_APP_TOKEN = os.getenv("SODA_APP_TOKEN")


def validate_config():
    if not CONNECT_STR:
        logger.error("Missing 'AZURE_STORAGE_CONNECTION_STRING' environment variable")
        raise SystemExit(1)


@retry(tries=5, delay=5.0, backoff=2.0)
def download_and_stream_sample(limit: int):
    client = Socrata(SODA_DOMAIN, SODA_APP_TOKEN, timeout=120)
    tmp = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json")
    tmp_path = tmp.name
    try:
        tmp.write("[")
        first = True
        offset = 0
        remaining = limit
        while remaining > 0:
            page_size = min(CHUNK_SIZE, remaining)
            page = client.get(SODA_DATASET_ID, limit=page_size, offset=offset)
            if not page:
                break
            for rec in page:
                if not first:
                    tmp.write(",\n")
                tmp.write(json.dumps(rec))
                first = False
            offset += len(page)
            remaining -= len(page)
            logger.info("Downloaded %s rows so far (remaining %s)", offset, remaining)
            if len(page) < page_size:
                break
        tmp.write("]")
        tmp.close()
        return tmp_path
    except Exception:
        try:
            tmp.close()
        except Exception:
            pass
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def upload_file_to_azure(path: str):
    logger.info("Uploading %s to Azure container %s", path, CONTAINER_NAME)
    blob_service_client = BlobServiceClient.from_connection_string(CONNECT_STR)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_FILE_NAME)
    with open(path, "rb") as fh:
        blob_client.upload_blob(fh, overwrite=True)


def main():
    logger.info("Starting TNP Trips extract")
    validate_config()
    tmp_path = None
    try:
        tmp_path = download_and_stream_sample(DATASET_LIMIT)
        upload_file_to_azure(tmp_path)
        logger.info("TNP Trips extract finished successfully")
    except Exception as exc:
        logger.exception("TNP Trips extract failed: %s", exc)
        raise SystemExit(1)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


if __name__ == "__main__":
    main()