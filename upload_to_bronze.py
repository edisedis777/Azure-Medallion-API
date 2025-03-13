# upload_to_bronze.py
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

# Azure Data Lake Storage details
ACCOUNT_NAME = "<your-storage-account-name>"
CONTAINER_NAME = "bronze"
BLOB_NAME = "random_data.csv"
LOCAL_FILE_PATH = "random_data.csv"

# Authenticate using Azure credentials
credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(
    account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
    credential=credential
)

# Upload the file
container_client = blob_service_client.get_container_client(CONTAINER_NAME)
with open(LOCAL_FILE_PATH, "rb") as data:
    blob_client = container_client.get_blob_client(BLOB_NAME)
    blob_client.upload_blob(data, overwrite=True)

print(f"Uploaded {LOCAL_FILE_PATH} to {CONTAINER_NAME}/{BLOB_NAME}")
