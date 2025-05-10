from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
from azure.identity import ClientSecretCredential
import requests
import os

# Replace with your paths, credentials, and settings
DBT_PROJECT_DIR = "/path/to/your/dbt/project"
BRONZE_PATH = "path_to_bronze_data"
SILVER_PATH = "path_to_silver_data"
SYNAPSE_PIPELINE_ID = "your_synapse_pipeline_id"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'github_etl_dag',
    default_args=default_args,
    description='Ingest, Transform, Synapse, DBT',
    schedule_interval=timedelta(hours=1),  # Run every hour
    catchup=False,
)

def fetch_github_data():
    subprocess.run(["python", "/home/azureUser/ETL-Github-Pipeline/ingestion/ingest_github.py"], check=True)  
    pass

def run_transformation():
    subprocess.run(["python", "/home/azureUser/ETL-Github-Pipeline/transforms/bronze_to_silver/transform.py"], check=True)
    pass

def trigger_synapse_pipeline():
    # Auth info
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    synapse_workspace = os.getenv("SYNAPSE_WORKSPACE")  
    pipeline_name = os.getenv("SYNAPSE_PIPELINE_NAME")  
    subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
    resource_group = os.getenv("AZURE_RESOURCE_GROUP")

    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    token = credential.get_token("https://dev.azuresynapse.net/.default").token

    url = f"https://{synapse_workspace}.dev.azuresynapse.net/pipelines/{pipeline_name}/createRun?api-version=2020-12-01"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers)

    if response.status_code == 200:
        print("Synapse pipeline triggered successfully.")
    else:
        raise Exception(f"Failed to trigger Synapse pipeline: {response.text}")



def run_dbt():
    try:
        logging.info("Running dbt models...")
        result = subprocess.run(
            ["dbt", "run", "--project-dir", "transforms/silver_to_gold/silver_to_gold_dbt"],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info(f"dbt output:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"dbt run failed:\n{e.stderr}")
        raise

ingest_task = PythonOperator(
    task_id='fetch_github_data',
    python_callable=fetch_github_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='run_transformation',
    python_callable=run_transformation,
    dag=dag,
)

trigger_synapse_task = PythonOperator(
    task_id='trigger_synapse_pipeline',
    python_callable=trigger_synapse_pipeline,
    dag=dag,
)

dbt_task = PythonOperator(
    task_id='run_dbt',
    python_callable=run_dbt,
    dag=dag,
)

# Set up task dependencies: Ensure each task runs in sequence
ingest_task >> transform_task >> trigger_synapse_task >> dbt_task
