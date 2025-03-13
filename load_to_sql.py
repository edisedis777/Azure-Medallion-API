# load_to_sql.py
import os
import tempfile
import pandas as pd
from sqlalchemy import create_engine, text
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential, AzureCliCredential, ManagedIdentityCredential
from azure.core.exceptions import ClientAuthenticationError
import logging
from azure.keyvault.secrets import SecretClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ACCOUNT_NAME = os.environ.get("STORAGE_ACCOUNT_NAME")
CONTAINER_NAME = "gold"
BLOB_NAME = "purchases.parquet"
KEY_VAULT_URL = os.environ.get("KEY_VAULT_URL")

def get_credential():
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

def get_db_connection_string():
    credential = get_credential()
    if KEY_VAULT_URL:
        secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
        try:
            username = secret_client.get_secret("db-username").value
            password = secret_client.get_secret("db-password").value
            server = secret_client.get_secret("db-server").value
            database = secret_client.get_secret("db-database").value
        except Exception as e:
            logging.error(f"Error retrieving secrets from Key Vault: {str(e)}")
            raise
    else:
        username = os.environ.get("DB_USERNAME")
        password = os.environ.get("DB_PASSWORD")
        server = os.environ.get("DB_SERVER")
        database = os.environ.get("DB_DATABASE")
        if not all([username, password, server, database]):
            raise ValueError("Database credentials not provided in environment variables")
    
    return f"mssql+pyodbc://{username}:{password}@{server}.database.windows.net:1433/{database}?driver=ODBC+Driver+17+for+SQL+Server"

def create_db_views(engine):
    views = [
        """
        CREATE OR ALTER VIEW sales_by_type AS
        SELECT PurchaseType, SUM(TotalAmount) AS TotalSales
        FROM purchases
        GROUP BY PurchaseType
        """,
        """
        CREATE OR ALTER VIEW sales_by_city AS
        SELECT City, SUM(TotalAmount) AS TotalSales
        FROM purchases
        GROUP BY City
        """
    ]
    with engine.connect() as conn:
        for view_sql in views:
            conn.execute(text(view_sql))
            conn.commit()
    logging.info("Database views created successfully")

def main():
    if not ACCOUNT_NAME:
        raise ValueError("STORAGE_ACCOUNT_NAME environment variable is not set")
    
    credential = get_credential()
    blob_service_client = BlobServiceClient(
        account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
        credential=credential
    )
    
    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as temp_file:
        temp_path = temp_file.name
    
    try:
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)
        with open(temp_path, "wb") as f:
            f.write(blob_client.download_blob().readall())
        logging.info(f"Downloaded Gold data to {temp_path}")
        
        db_conn_string = get_db_connection_string()
        engine = create_engine(db_conn_string)
        
        batch_size = 10000
        for i, df_chunk in enumerate(pd.read_parquet(temp_path, chunksize=batch_size)):
            df_chunk.to_sql("purchases", engine, if_exists="replace" if i == 0 else "append", index=False)
            logging.info(f"Processed batch {i+1} ({len(df_chunk)} records)")
        
        create_db_views(engine)
        logging.info("Data loaded into Azure SQL Database")
    except Exception as e:
        logging.error(f"Error loading data to SQL: {str(e)}")
        raise
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)

if __name__ == "__main__":
    main()
