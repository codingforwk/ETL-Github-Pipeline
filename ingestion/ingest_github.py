import os
import json
import requests
import logging
from datetime import datetime, timedelta
from azure.storage.filedatalake import DataLakeServiceClient
from jsonschema import validate

#logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
            "created_at": {"type": "string", "format": "date-time"},
        }
    }
}

def fetch_github_events(hour_utc: str) -> bytes:
    """
    Fetches GitHub events from the API.
    :param hour_utc: The UTC hour to fetch events for.
    :return: The response content in bytes.
    """
    url = f"https://data.gharchive.org/{hour_utc}.json.gz"
    logger.info(f"Fetching data from {url}")

    try: 
        resposnse = requests.get(url, stream=True, timeout=30)
        resposnse.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from GitHub: {str(e)}")
        raise

def validate_json_schema(data: bytes) -> None:
    """
    Validates the JSON data against the schema.
    :param data: The JSON data in bytes.
    :return: None
    """
    try:
        json_data = json.loads(data.decode('utf-8'))
        # Validate the JSON data against the schema
        validate(instance=json_data, schema=RAW_SCHEMA)
        logger.info("JSON data is valid.")
    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Schema validation error: {str(e)}")
        raise

def upload_to_adls(data: bytes, file_path: str) -> None:
    """
    Uploads the data to Azure Data Lake Storage.
    :param data: The data to upload in bytes.
    :param file_path: The path in ADLS where the data will be stored.
    :return: None
    """

        # Initialize the DataLakeServiceClient
    service_client = DataLakeServiceClient(
        account_url=f"https://{os.environ['AZURE_STORAGE_ACCOUNT']}.dfs.core.windows.net",
        credential=os.environ['AZURE_STORAGE_KEY']
    )
    file_system_client = service_client.get_file_system_client("raw-events")

    try:
        file_client = file_system_client.get_file_client(file_path)
        file_client.create_file()
        file_client.append(data, 0, len(data))
        file_client.flush_data(len(data))
        logger.info(f'Uploaded {len(data)} bytes to {file_path}')
    except Exception as e:
        logger.error(f"Error uploading to ADLS: {str(e)}")
        raise

def main():
    """
    Main function to orchestrate the ingestion process.
    :return: None
    """
    try:
        #  get data from the last hour (GH archive delay)
        target_time = datetime.now(datetime.timezone.utc) - timedelta(hours=2)
        hour_str = target_time.strftime("%Y-%m-%d-%H")

        raw_data = fetch_github_events(hour_str)
        validate_json_schema(raw_data)

        file_path = (
            f"year={target_time.year}/"
            f"month={target_time.month:02d}/"
            f"day={target_time.day:02d}/"
            f"data_{hour_str}.json.gz"  
        )


        upload_to_adls(raw_data, file_path)

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
    

