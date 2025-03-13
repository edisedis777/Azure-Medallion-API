# api.py
from fastapi import FastAPI, HTTPException, Depends, Request
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import os
from typing import Optional
import uuid
import re
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI()

# Environment configuration
ENV = os.getenv("ENVIRONMENT", "development")

# Azure Key Vault configuration
KEYVAULT_URL = os.getenv("KEYVAULT_URL")

# Configure Azure credentials with fallback strategy
def get_azure_credential():
    try:
        # Try DefaultAzureCredential first
        credential = DefaultAzureCredential()
        # Test the credential
        credential.get_token("https://management.azure.com/.default")
        logger.info("Using DefaultAzureCredential for authentication")
        return credential
    except Exception as e:
        logger.warning(f"DefaultAzureCredential failed: {str(e)}")
        
        # Fallback to ClientSecretCredential if environment variables are available
        client_id = os.getenv("AZURE_CLIENT_ID")
        client_secret = os.getenv("AZURE_CLIENT_SECRET")
        tenant_id = os.getenv("AZURE_TENANT_ID")
        
        if client_id and client_secret and tenant_id:
            logger.info("Falling back to ClientSecretCredential")
            return ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
        else:
            raise Exception("No valid Azure credentials available")

# Get database credentials from Key Vault or environment variables
def get_db_config():
    if KEYVAULT_URL:
        try:
            credential = get_azure_credential()
            secret_client = SecretClient(vault_url=KEYVAULT_URL, credential=credential)
            
            username = secret_client.get_secret("db-username").value
            password = secret_client.get_secret("db-password").value
            server = secret_client.get_secret("db-server").value
            database = secret_client.get_secret("db-database").value
            
            logger.info("Retrieved database credentials from Key Vault")
        except Exception as e:
            logger.error(f"Failed to retrieve secrets from Key Vault: {str(e)}")
            raise
    else:
        # Fallback to environment variables
        username = os.getenv("DB_USERNAME")
        password = os.getenv("DB_PASSWORD")
        server = os.getenv("DB_SERVER")
        database = os.getenv("DB_DATABASE")
        
        if not all([username, password, server, database]):
            raise Exception("Database configuration incomplete. Set environment variables or configure Key Vault.")
        
        logger.info("Using database credentials from environment variables")
    
    return {
        "username": username,
        "password": password,
        "server": server,
        "database": database
    }

# Create database engine
def get_db_engine():
    db_config = get_db_config()
    connection_string = f"mssql+pyodbc://{db_config['username']}:{db_config['password']}@{db_config['server']}.database.windows.net:1433/{db_config['database']}?driver=ODBC+Driver+17+for+SQL+Server"
    return create_engine(connection_string)

# Input validation functions
def validate_id(id_value: str) -> bool:
    """Validate a UUID format."""
    try:
        uuid.UUID(id_value)
        return True
    except ValueError:
        return False

def validate_name(name: str) -> bool:
    """Validate a name contains only allowed characters."""
    return bool(re.match(r'^[A-Za-z\-\s]{1,50}$', name))

# Helper function to query the database with proper parameter binding
def query_db(query, params=None):
    engine = get_db_engine()
    try:
        with engine.connect() as connection:
            result = pd.read_sql_query(text(query), connection, params=params)
        return result.to_dict(orient="records")
    except SQLAlchemyError as e:
        logger.error(f"Database error: {str(e)}")
        raise HTTPException(status_code=500, detail="Database error occurred")

# Request ID middleware for idempotency
@app.middleware("http")
async def add_request_id(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID")
    if not request_id:
        request_id = str(uuid.uuid4())
    
    # You can implement a cache or database check here to enforce idempotency
    # For example:
    # if request.method in ["POST", "PUT", "PATCH"] and is_duplicate_request(request_id):
    #     return JSONResponse(status_code=409, content={"detail": "Duplicate request detected"})
    
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response

# Endpoint: Get purchase by UniqueID or LastName
@app.get("/purchases/")
def get_purchase(unique_id: Optional[str] = None, lastname: Optional[str] = None):
    # Input validation
    if unique_id and not validate_id(unique_id):
        raise HTTPException(status_code=400, detail="Invalid unique_id format")
    
    if lastname and not validate_name(lastname):
        raise HTTPException(status_code=400, detail="Invalid lastname format")
    
    if unique_id:
        query = "SELECT * FROM purchases WHERE UniqueID = :unique_id"
        params = {"unique_id": unique_id}
        result = query_db(query, params)
        if not result:
            raise HTTPException(status_code=404, detail="Purchase not found")
        return result[0]
    elif lastname:
        query = "SELECT * FROM purchases WHERE LastName = :lastname"
        params = {"lastname": lastname}
        result = query_db(query, params)
        if not result:
            raise HTTPException(status_code=404, detail="No purchases found for this last name")
        return result
    else:
        raise HTTPException(status_code=400, detail="Please provide unique_id or lastname")

# Endpoint: Get sales by purchase type
@app.get("/analytics/sales_by_type")
def get_sales_by_type():
    query = "SELECT * FROM sales_by_type"
    result = query_db(query)
    return result

# Endpoint: Get sales by city
@app.get("/analytics/sales_by_city")
def get_sales_by_city():
    query = "SELECT * FROM sales_by_city"
    result = query_db(query)
    return result

# Run the app locally
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
