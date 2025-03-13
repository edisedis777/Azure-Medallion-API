# Azure-Medallion-API

A data pipeline using the Medallion architecture (Bronze, Silver, Gold layers) on Microsoft Azure, with a RESTful API built using FastAPI to serve the processed data. 
It ingests customer purchase data from a CSV file, processes it through multiple stages, and exposes it via an API.

## Project Structure
- **Bronze Layer**: Raw data ingestion into Azure Data Lake Storage.
- **Silver Layer**: Data cleaning and transformation using Azure Databricks.
- **Gold Layer**: Final processed data for consumption, stored in Azure Data Lake and loaded into Azure SQL Database.
- **API**: FastAPI application to query purchase data and analytics.

## Files
- **`upload_to_bronze.py`**: Uploads `random_data.csv` to the Bronze layer in Azure Data Lake Storage.
- **`silver_processing.py`**: Databricks notebook to clean and transform raw data into the Silver layer.
- **`gold_processing.py`**: Databricks notebook to process Silver data into the Gold layer.
- **`load_to_sql.py`**: Loads Gold data into Azure SQL Database and creates analytics views.
- **`api.py`**: FastAPI application to serve purchase data and analytics.

## Prerequisites
- **Azure Services**:
  - Azure Data Lake Storage (containers: `bronze`, `silver`, `gold`)
  - Azure Databricks
  - Azure SQL Database
  - Azure Key Vault (optional for secrets)
- **Local Environment**:
  - Python 3.9+
  - ODBC Driver 17 for SQL Server
  - Install dependencies:
    ```bash
    pip install pandas pyspark pyarrow fastapi uvicorn sqlalchemy pyodbc azure-storage-blob azure-identity
    ```
## Configuration:
### Set environment variables:
- STORAGE_ACCOUNT_NAME: Azure Data Lake Storage account name
- KEY_VAULT_URL: Azure Key Vault URL (optional)
- DB_USERNAME, DB_PASSWORD, DB_SERVER, DB_DATABASE: SQL Database credentials (if not using Key Vault)
- AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID: For ClientSecretCredential (optional)
- Databricks secret scope medallion-kv with key storage-account-name.

## Setup Instructions
1. Prepare Data:
- Save your customer purchase data as random_data.csv locally.
- Ensure it matches the expected schema (see silver_processing.py).

2. Upload to Bronze:
```bash
python upload_to_bronze.py
```
- Uploads random_data.csv to bronze/random_data.csv.

3. Process to Silver:
- Run silver_processing.py in a Databricks notebook.
- Outputs cleaned_data.parquet in silver/.

4. Process to Gold:
- Run gold_processing.py in a Databricks notebook.
- Outputs purchases.parquet in gold/.

5. Load to SQL:
```bash
python load_to_sql.py
```
- Loads data into Azure SQL Database table purchases and creates views sales_by_type and sales_by_city.

6. Run API:
```bash
python api.py
```
- Starts the FastAPI server at http://localhost:8000.

## API Endpoints
- GET /purchases/?unique_id={unique_id}:
  - Returns a single purchase by UniqueID.
  - Example: http://localhost:8000/purchases?unique_id=e7a8b3ee-72ee-4767-bfd0-66b44b789fa7
- GET /purchases/?lastname={lastname}:
  - Returns all purchases for a given LastName (case-insensitive).
  - Example: http://localhost:8000/purchases?lastname=Bauer
- GET /analytics/sales_by_type:
  - Returns total sales by purchase type.
  - Example: http://localhost:8000/analytics/sales_by_type
- GET /analytics/sales_by_city:
  - Returns total sales by city.
  - Example: http://localhost:8000/analytics/sales_by_city

## Deployment (Optional)
To deploy on an Azure VM:

1. Create an Ubuntu VM in Azure.
2. SSH into the VM:
```bash
ssh <username>@<vm-public-ip>
```
3. Install dependencies:
```bash
sudo apt update
sudo apt install python3-pip
pip3 install -r requirements.txt
```
4. Create requirements.txt with the listed dependencies.
Copy files to the VM (e.g., via SCP):
```bash
scp api.py <username>@<vm-public-ip>:~/api.py
```
5. Run the API:
```bash
nohup uvicorn api:app --host 0.0.0.0 --port 80 &
```
6. Open port 80 in Azure portal (Networking > Inbound rules).
7. Access at http://<vm-public-ip>/.

## Notes
- Schema Limitation: silver_processing.py excludes some CSV fields (e.g., Telephone, Email). Update the schema if needed.
- Error Handling: Scripts include logging and exception handling for debugging.
- Security: Use Key Vault for sensitive credentials in production.

## Troubleshooting
- ODBC Error: Install ODBC Driver 17 for SQL Server (sudo apt install -y msodbcsql17 on Ubuntu).
- Secret Scope: Configure medallion-kv in Databricks with storage-account-name.
- Key Vault: Ensure secrets (db-username, etc.) are set if using KEY_VAULT_URL.
