import os
import requests
from azure.identity import ClientSecretCredential

# Load environment variables
TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID")
RESOURCE_GROUP = os.getenv("AZURE_RESOURCE_GROUP")
WORKSPACE = os.getenv("SYNAPSE_WORKSPACE")
PIPELINE_NAME = os.getenv("SYNAPSE_PIPELINE_NAME")

# Authenticate and get access token
credential = ClientSecretCredential(
    tenant_id=TENANT_ID,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET
)

token = credential.get_token("https://dev.azuresynapse.net/.default")
access_token = token.token

# Build the URL to trigger the pipeline
url = (
    f"https://{WORKSPACE}.dev.azuresynapse.net/pipelines/{PIPELINE_NAME}/createRun"
    f"?api-version=2020-12-01"
)

# Headers
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# Optional: if your pipeline needs parameters
payload = {}  # You can pass {"param1": "value"} if needed

# Trigger the pipeline
response = requests.post(url, headers=headers, json=payload)

# Check result
if response.status_code == 200:
    run_id = response.json()["runId"]
    print(f"✅ Pipeline triggered successfully. Run ID: {run_id}")
else:
    print(f"❌ Failed to trigger pipeline. Status: {response.status_code}")
    print(response.text)
