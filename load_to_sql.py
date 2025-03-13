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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Azure configuration - from environment variables or Key Vault
ACCOUNT_NAME = os.environ.get("STORAGE_ACCOUNT_NAME")
CONTAINER_NAME = "gold"
BLOB_NAME = "purchases.parquet"
KEY_VAULT_URL = os.environ.get("KEY_VAULT_URL")

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

def get_db_connection_string():
    """Get database connection string from Key Vault."""
    if not KEY_VAULT_URL:
        raise ValueError("KEY_VAULT_URL environment variable is not set")
    
    credential = get_credential()
    secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
    
    try:
        # Get connection string components from Key Vault
        username = secret_client.get_secret("sql-username").value
        password = secret_client.get_secret("sql-password").value
        server = secret_client.get_secret("sql-server").value
        database = secret_client.get_secret("sql-database").value
        
        # Build connection string
        conn_str = f"mssql+pyodbc://{username}:{password}@{server}.database.windows.net:1433/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        return conn_str
    except Exception as e:
        logging.error(f"Error retrieving database credentials: {str(e)}")
        raise

def create_db_views(engine):
    """Create SQL views for analytics."""
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
    
    try:
        with engine.connect() as conn:
            for view_sql in views:
                conn.execute(text(view_sql))
                conn.commit()
        logging.info("Database views created successfully")
    except Exception as e:
        logging.error(f"Error creating database views: {str(e)}")
        raise

def main():
    """Main function to load data from Gold layer to SQL Database."""
    if not ACCOUNT_NAME:
        raise ValueError("STORAGE_ACCOUNT_NAME environment variable is not set")
    
    try:
        # Authenticate
        credential = get_credential()
        
        # Create blob service client
        blob_service_client = BlobServiceClient(
            account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
            credential=credential
        )
        
        # Download Parquet from Gold layer using a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as temp_file:
            temp_path = temp_file.name
            
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)
        with open(temp_path, "wb") as f:
            download_stream = blob_client.download_blob()
            f.write(download_stream.readall())
        
        logging.info(f"Downloaded Gold data to temporary file: {temp_path}")
        
        # Get database connection string
        db_conn_string = get_db_connection_string()
        engine = create_engine(db_conn_string)
        
        # Process in batches to avoid loading everything into memory
        batch_size = 10000
        for i, df_chunk in enumerate(pd.read_parquet(temp_path, chunksize=batch_size)):
            # Write to SQL Database
            if i == 0:
                # First chunk replaces the table
                df_chunk.to_sql("purchases", engine, if_exists="replace", index=False)
            else:
                # Subsequent chunks append
                df_chunk.to_sql("purchases", engine, if_exists="append", index=False)
            
            logging.info(f"Processed batch {i+1} ({len(df_chunk)} records)")
        
        # Create views
        create_db_views(engine)
        
        # Clean up temporary file
        os.unlink(temp_path)
        
        logging.info("Data loaded into Azure SQL Database and views created")
        
    except Exception as e:
        logging.error(f"Error loading data to SQL: {str(e)}")
        # Clean up temporary file in case of error
        if 'temp_path' in locals() and os.path.exists(temp_path):
            os.unlink(temp_path)
        raise

if __name__ == "__main__":
    main()
