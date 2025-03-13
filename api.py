# api.py
from fastapi import FastAPI, HTTPException, Request
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
ENV = os.getenv("ENVIRONMENT", "development")
KEYVAULT_URL = os.getenv("KEYVAULT_URL")

def get_azure_credential():
    try:
        credential = DefaultAzureCredential()
        credential.get_token("https://management.azure.com/.default")
        logger.info("Using DefaultAzureCredential")
        return credential
    except Exception as e:
        logger.warning(f"DefaultAzureCredential failed: {str(e)}")
        client_id = os.getenv("AZURE_CLIENT_ID")
        client_secret = os.getenv("AZURE_CLIENT_SECRET")
        tenant_id = os.getenv("AZURE_TENANT_ID")
        if client_id and client_secret and tenant_id:
            logger.info("Falling back to ClientSecretCredential")
            return ClientSecretCredential(tenant_id, client_id, client_secret)
        raise Exception("No valid Azure credentials available")

def get_db_config():
    if KEYVAULT_URL:
        credential = get_azure_credential()
        secret_client = SecretClient(vault_url=KEYVAULT_URL, credential=credential)
        try:
            return {
                "username": secret_client.get_secret("db-username").value,
                "password": secret_client.get_secret("db-password").value,
                "server": secret_client.get_secret("db-server").value,
                "database": secret_client.get_secret("db-database").value
            }
        except Exception as e:
            logger.error(f"Key Vault error: {str(e)}")
            raise
    else:
        config = {
            "username": os.getenv("DB_USERNAME"),
            "password": os.getenv("DB_PASSWORD"),
            "server": os.getenv("DB_SERVER"),
            "database": os.getenv("DB_DATABASE")
        }
        if not all(config.values()):
            raise Exception("Database configuration incomplete")
        logger.info("Using environment variables for DB config")
        return config

def get_db_engine():
    db_config = get_db_config()
    return create_engine(
        f"mssql+pyodbc://{db_config['username']}:{db_config['password']}@{db_config['server']}.database.windows.net:1433/{db_config['database']}?driver=ODBC+Driver+17+for+SQL+Server"
    )

def validate_id(id_value: str) -> bool:
    try:
        uuid.UUID(id_value)
        return True
    except ValueError:
        return False

def validate_name(name: str) -> bool:
    return bool(re.match(r'^[A-Za-z\-\s]{1,50}$', name))

def query_db(query, params=None):
    engine = get_db_engine()
    try:
        with engine.connect() as connection:
            result = pd.read_sql_query(text(query), connection, params=params)
        return result.to_dict(orient="records")
    except SQLAlchemyError as e:
        logger.error(f"Database error: {str(e)}")
        raise HTTPException(status_code=500, detail="Database error occurred")

@app.middleware("http")
async def add_request_id(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response

@app.get("/purchases/")
def get_purchase(unique_id: Optional[str] = None, lastname: Optional[str] = None):
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
        query = "SELECT * FROM purchases WHERE UPPER(LastName) = UPPER(:lastname)"
        params = {"lastname": lastname}
        result = query_db(query, params)
        if not result:
            raise HTTPException(status_code=404, detail="No purchases found for this last name")
        return result
    else:
        raise HTTPException(status_code=400, detail="Please provide unique_id or lastname")

@app.get("/analytics/sales_by_type")
def get_sales_by_type():
    return query_db("SELECT * FROM sales_by_type")

@app.get("/analytics/sales_by_city")
def get_sales_by_city():
    return query_db("SELECT * FROM sales_by_city")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
