# load_to_sql.py
import os
import tempfile
import pyarrow.parquet as pq
import pyarrow as pa
from sqlalchemy import create_engine, text
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential, AzureCliCredential, ManagedIdentityCredential
from azure.core.exceptions import ClientAuthenticationError
import logging
from azure.keyvault.secrets import SecretClient
import pyodbc

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

def create_table_if_not_exists(engine, table_name, columns):
    column_definitions = []
    for col_name, col_type in columns:
        column_definitions.append(f"{col_name} {col_type}")
    
    create_table_sql = f"""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')
    BEGIN
        CREATE TABLE {table_name} (
            {', '.join(column_definitions)}
        )
    END
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
    logging.info(f"Ensured table {table_name} exists")

def map_arrow_to_sql_type(arrow_type):
    if pa.types.is_integer(arrow_type):
        return "INT"
    elif pa.types.is_floating(arrow_type):
        return "FLOAT"
    elif pa.types.is_string(arrow_type):
        return "NVARCHAR(MAX)"
    elif pa.types.is_boolean(arrow_type):
        return "BIT"
    elif pa.types.is_timestamp(arrow_type):
        return "DATETIME2"
    elif pa.types.is_date(arrow_type):
        return "DATE"
    else:
        return "NVARCHAR(MAX)"

def batch_insert_data(engine, connection_string, table_name, parquet_file, batch_size=10000):
    # Read parquet schema
    parquet_schema = pq.read_schema(parquet_file)
    
    # Map PyArrow schema to SQL types and create table if not exists
    columns = [(field.name, map_arrow_to_sql_type(field.type)) for field in parquet_schema]
    create_table_if_not_exists(engine, table_name, columns)
    
    # Create connection for fast bulk insert
    conn = pyodbc.connect(connection_string.replace('mssql+pyodbc://', ''))
    cursor = conn.cursor()
    
    # Read parquet file in batches
    parquet_reader = pq.ParquetFile(parquet_file)
    first_batch = True
    
    # Process each batch
    for batch_number, batch in enumerate(parquet_reader.iter_batches(batch_size=batch_size)):
        table = pa.Table.from_batches([batch])
        
        # Clear table on first batch
        if first_batch:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            conn.commit()
            first_batch = False
        
        # Prepare values for insertion
        rows = []
        for i in range(len(table)):
            row = []
            for j in range(len(table.column_names)):
                if table[j][i].as_py() is None:
                    row.append("NULL")
                elif isinstance(table[j][i].as_py(), (int, float, bool)):
                    row.append(str(table[j][i].as_py()))
                else:
                    # Escape single quotes in string values
                    value = str(table[j][i].as_py()).replace("'", "''")
                    row.append(f"'{value}'")
            rows.append(f"({', '.join(row)})")
        
        # Insert batch
        if rows:
            batch_values = ', '.join(rows)
            column_names = ', '.join(table.column_names)
            insert_sql = f"INSERT INTO {table_name} ({column_names}) VALUES {batch_values}"
            
            # Execute in smaller chunks if needed
            try:
                cursor.execute(insert_sql)
                conn.commit()
                logging.info(f"Inserted batch {batch_number+1} ({len(rows)} records)")
            except pyodbc.Error as e:
                # If the batch is too large, we may need to split it
                if "statement too long" in str(e).lower():
                    # Split into smaller batches
                    for i in range(0, len(rows), 1000):
                        mini_batch = ', '.join(rows[i:i+1000])
                        mini_insert_sql = f"INSERT INTO {table_name} ({column_names}) VALUES {mini_batch}"
                        cursor.execute(mini_insert_sql)
                        conn.commit()
                    logging.info(f"Inserted batch {batch_number+1} in multiple chunks ({len(rows)} records total)")
                else:
                    logging.error(f"Error inserting batch: {str(e)}")
                    raise
    
    cursor.close()
    conn.close()

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
        
        # Load data using PyArrow batches
        batch_insert_data(engine, db_conn_string, "purchases", temp_path, batch_size=10000)
        
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
