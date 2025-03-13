# load_to_sql.py
import pandas as pd
from sqlalchemy import create_engine
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

# Azure Data Lake Storage details
ACCOUNT_NAME = "<your-storage-account-name>"
CONTAINER_NAME = "gold"
BLOB_NAME = "purchases.parquet"

# SQL Database connection string
DATABASE_URL = "mssql+pyodbc://<username>:<password>@<server>.database.windows.net:1433/<database>?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(DATABASE_URL)

# Download Parquet from Gold layer
credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(
    account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
    credential=credential
)
blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)
with open("purchases.parquet", "wb") as f:
    blob_client.download_blob().readinto(f)

# Read Parquet into pandas
df = pd.read_parquet("purchases.parquet")

# Write to SQL Database
df.to_sql("purchases", engine, if_exists="replace", index=False)

# Create views
with engine.connect() as conn:
    conn.execute("""
        CREATE VIEW sales_by_type AS
        SELECT PurchaseType, SUM(TotalAmount) AS TotalSales
        FROM purchases
        GROUP BY PurchaseType
    """)
    conn.execute("""
        CREATE VIEW sales_by_city AS
        SELECT City, SUM(TotalAmount) AS TotalSales
        FROM purchases
        GROUP BY City
    """)

print("Data loaded into Azure SQL Database and views created")
