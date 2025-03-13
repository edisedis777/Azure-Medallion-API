# upload_to_bronze.py
import os
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential, AzureCliCredential, ManagedIdentityCredential
from azure.core.exceptions import ClientAuthenticationError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Azure Data Lake Storage details
ACCOUNT_NAME = os.environ.get("STORAGE_ACCOUNT_NAME")
CONTAINER_NAME = "bronze"
BLOB_NAME = "random_data.csv"
LOCAL_FILE_PATH = "random_data.csv"

def get_credential():
    """Try multiple authentication methods in order of preference."""
    credential_types = [
        (DefaultAzureCredential, "DefaultAzureCredential"),
        (AzureCliCredential, "AzureCliCredential"),
        (ManagedIdentityCredential, "ManagedIdentityCredential")
    ]
    
    for cred_class, cred_name in credential_types:
        try:
            credential = cred_class()
            logging.info(f"Successfully authenticated using {cred_name}")
            return credential
        except ClientAuthenticationError as e:
            logging.warning(f"Authentication failed with {cred_name}: {str(e)}")
    
    raise Exception("All authentication methods failed")

def main():
    """Main function to upload files to bronze container."""
    if not ACCOUNT_NAME:
        raise ValueError("STORAGE_ACCOUNT_NAME environment variable is not set")
    
    try:
        # Authenticate using Azure credentials
        credential = get_credential()
        
        # Create blob service client
        blob_service_client = BlobServiceClient(
            account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
            credential=credential
        )
        
        # Check if container exists, create if not
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        try:
            container_client.get_container_properties()
        except Exception:
            logging.info(f"Container {CONTAINER_NAME} does not exist. Creating...")
            container_client.create_container()
        
        # Check if local file exists
        if not os.path.exists(LOCAL_FILE_PATH):
            raise FileNotFoundError(f"Local file {LOCAL_FILE_PATH} not found")
        
        # Upload the file with idempotency check
        with open(LOCAL_FILE_PATH, "rb") as data:
            blob_client = container_client.get_blob_client(BLOB_NAME)
            # Check if blob already exists
            try:
                blob_client.get_blob_properties()
                logging.info(f"Blob {BLOB_NAME} already exists. Overwriting...")
            except Exception:
                logging.info(f"Blob {BLOB_NAME} does not exist. Creating new...")
            
            blob_client.upload_blob(data, overwrite=True)
        
        logging.info(f"Uploaded {LOCAL_FILE_PATH} to {CONTAINER_NAME}/{BLOB_NAME}")
        
    except Exception as e:
        logging.error(f"Error uploading file: {str(e)}")
        raise

if __name__ == "__main__":
    main()
