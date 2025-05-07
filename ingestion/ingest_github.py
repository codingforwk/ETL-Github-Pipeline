import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import io
import json
import logging
import gzip
import requests
from datetime import datetime, timezone
from azure.storage.filedatalake import DataLakeServiceClient
from jsonschema import validate, ValidationError
from requests.exceptions import HTTPError

from utils.config_loader import load_config, get_env_or_config

# Configure logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# JSON schema for validating individual GitHub events
RAW_SCHEMA = {
    "type": "object",
    "required": ["id", "type", "actor", "repo", "created_at"],
    "properties": {
        "id": {"type": "string"},
        "type": {"type": "string"},
        "actor": {"type": "object"},
        "repo": {"type": "object"},
        "created_at": {"type": "string", "format": "date-time"}
    }
}

def fetch_github_events(url: str) -> bytes | None:
    """
    Download the .json.gz file from the given URL.

    Args:
        url (str): Full URL to the GitHub archive .json.gz file.

    Returns:
        bytes or None: Raw bytes of the gzipped file, or None if 404 (not available yet).

    Raises:
        HTTPError: For HTTP errors other than 404.
        Exception: For other unexpected errors.
    """
    logger.info(f"Fetching data from {url}")
    try:
        resp = requests.get(url, stream=True, timeout=30)
        resp.raise_for_status()
        logger.info(f"Successfully fetched data from {url}")
        return resp.content
    except HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(f"No data available at {url} (404). Will retry later.")
            return None
        logger.error(f"HTTP error fetching data: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching data: {e}")
        raise



def validate_json_schema(raw_bytes: bytes) -> None:
    valid_count = 0
    with gzip.GzipFile(fileobj=io.BytesIO(raw_bytes)) as gz:
        for idx, line in enumerate(gz, start=1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
                validate(instance=event, schema=RAW_SCHEMA)
                valid_count += 1
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON at line {idx}: {e}")
                raise
            except ValidationError as e:
                logger.error(f"Schema violation at line {idx}: {e.message}")
                raise
    logger.info(f"Validated {valid_count} events successfully")


def upload_to_adls(raw_bytes: bytes, path: str, storage_account: str, storage_key: str, file_system: str) -> None:
    """
    Upload the gzipped bytes to Azure Data Lake Storage Gen2.

    Args:
        raw_bytes (bytes): Gzipped data bytes.
        path (str): Destination path in ADLS (including partitions).
        storage_account (str): Azure Storage account name.
        storage_key (str): Azure Storage account key.
        file_system (str): ADLS filesystem/container name.

    Raises:
        Exception: For upload failures.
    """
    if not storage_account or not storage_key:
        raise ValueError("Missing storage account or storage key for ADLS upload")

    try:
        service_client = DataLakeServiceClient(
            account_url=f"https://{storage_account}.dfs.core.windows.net",
            credential=storage_key
        )
        file_client = service_client.get_file_system_client(file_system).get_file_client(path)
        file_client.upload_data(
            data=raw_bytes,
            overwrite=True,
            metadata={"source": "gharchive-ingestor"}
        )
        logger.info(f"Uploaded {len(raw_bytes)} bytes to ADLS path: {path}")
    except Exception as e:
        logger.error(f"Failed to upload data to ADLS: {e}")
        raise

def main():
    try:
        # Load config
        config = load_config()

        # Get Azure storage info from env or config
        storage_account = get_env_or_config("AZURE_STORAGE_ACCOUNT", config, "azure.storage_account")
        storage_key = os.getenv("AZURE_STORAGE_KEY")
        file_system = config.get("azure", {}).get("file_system", "raw-events")

        if not storage_account or not storage_key:
            raise ValueError("Azure Storage account or key not set in environment or config")

        # Get GitHub base URL and ingestion parameters
        github_base_url = config.get("api", {}).get("github_base_url", "https://data.gharchive.org")
        raw_data_prefix = config.get("paths", {}).get("raw_data_prefix")

        # Fixed ingestion datetime for testing (May 20, 2024, 12:00 UTC)
        target = datetime(2024, 5, 21, 12, 0, 0, tzinfo=timezone.utc)
        hour_str = target.strftime("%Y-%m-%d-%H")
        logger.info(f"Processing GitHub archive for fixed hour: {hour_str}")

        # Compose download URL
        url = f"{github_base_url}/{hour_str}.json.gz"

        # Fetch raw data
        raw_bytes = fetch_github_events(url)
        if raw_bytes is None:
            logger.info("Data not available yet. Exiting.")
            return

        # Validate JSON schema
        validate_json_schema(raw_bytes)

        # Compose ADLS path using config pattern
        path = raw_data_prefix.format(
            year=target.year,
            month=target.month,
            day=target.day,
            hour_str=hour_str
        )

        # Upload to ADLS
        upload_to_adls(raw_bytes, path, storage_account, storage_key, file_system)

        logger.info("Ingestion completed successfully.")

    except Exception as e:
        logger.error(f"Ingestion pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()
