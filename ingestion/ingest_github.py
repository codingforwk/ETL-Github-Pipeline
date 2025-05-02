import os
import json
import logging
import gzip
import requests
from datetime import datetime, timedelta, timezone
from azure.storage.filedatalake import DataLakeServiceClient
from jsonschema import validate
from requests.exceptions import HTTPError

# ──────────────────────────────────────────────────────────────────────────────
# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# JSON schema for raw GHArchive events
RAW_SCHEMA = {
    "type": "array",
    "items": {
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
}

# ──────────────────────────────────────────────────────────────────────────────
def fetch_github_events(hour_utc: str) -> bytes | None:
    """
    Download the .json.gz for a given UTC hour.
    Returns the raw bytes, or None if the hour isn’t published yet (404).
    """
    url = f"https://data.gharchive.org/{hour_utc}.json.gz"
    logger.info(f"Fetching data from {url}")

    try:
        resp = requests.get(url, stream=True, timeout=30)
        resp.raise_for_status()
        return resp.content

    except HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(f"No data for {hour_utc} (404). Will retry later.")
            return None
        logger.error(f"HTTP error fetching data: {e}")
        raise

    except Exception as e:
        logger.error(f"Unexpected error fetching data: {e}")
        raise

# ──────────────────────────────────────────────────────────────────────────────
def validate_json_schema(raw_bytes: bytes) -> None:
    """
    Decompresses the GZIP payload, splits into JSON lines,
    and validates against RAW_SCHEMA.
    """
# Update RAW_SCHEMA to validate individual events
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

def validate_json_schema(raw_bytes: bytes) -> None:
    """Validate JSONL format line-by-line"""
    decompressed = gzip.decompress(raw_bytes)
    lines = decompressed.decode().split('\n')
    
    valid_count = 0
    for idx, line in enumerate(lines, 1):
        if not line.strip():
            continue
            
        try:
            event = json.loads(line)
            validate(instance=event, schema=RAW_SCHEMA)
            valid_count += 1
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON at line {idx}")
            raise
        except ValidationError as e:
            logger.error(f"Schema violation at line {idx}: {str(e)}")
            raise

    logger.info(f"Validated {valid_count} events successfully")
# ──────────────────────────────────────────────────────────────────────────────
def upload_to_adls(raw_bytes: bytes, path: str) -> None:
    """
    Upload the gzipped bytes into Azure Data Lake Storage Gen2.
    """
    acct = os.getenv("AZURE_STORAGE_ACCOUNT")
    key = os.getenv("AZURE_STORAGE_KEY")

    if not acct or not key:
        raise ValueError("Missing AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_KEY")

    service_client = DataLakeServiceClient(
        account_url=f"https://{acct}.dfs.core.windows.net",
        credential=key
    )
    
    file_client = service_client.get_file_system_client("raw-events").get_file_client(path)
    
    # Single upload operation
    file_client.upload_data(
        data=raw_bytes,
        overwrite=True,
        metadata={"source": "gharchive-ingestor"}
    )
    logger.info(f"Uploaded {len(raw_bytes)} bytes to {path}")

# ──────────────────────────────────────────────────────────────────────────────
def main():
    # Compute the hour to fetch (3 hours ago UTC)
    
    target = datetime.now(timezone.utc) - timedelta(hours=3)
    hour_str = "2024-05-20-12"  # Known valid hour
    
    # Keep rest of your code...
    logger.info(f"OVERRIDE Processing hour: {hour_str}")

    # Fetch (or get None if not yet available)
    raw = fetch_github_events(hour_str)
    if raw is None:
        return  # no data published yet—exit cleanly

    # Validate & upload
    validate_json_schema(raw)
    dest_path = (
        f"year={target.year}/"
        f"month={target.month:02d}/"
        f"day={target.day:02d}/"
        f"data_{hour_str}.json.gz"
    )
    upload_to_adls(raw, dest_path)

if __name__ == "__main__":
    main()
