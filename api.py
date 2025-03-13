# api.py
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
import pandas as pd

# Initialize FastAPI app
app = FastAPI()

# Azure SQL Database connection string
DATABASE_URL = "mssql+pyodbc://<username>:<password>@<server>.database.windows.net:1433/<database>?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(DATABASE_URL)

# Helper function to query the database
def query_db(query, params=None):
    with engine.connect() as connection:
        result = pd.read_sql_query(text(query), connection, params=params)
    return result.to_dict(orient="records")

# Endpoint: Get purchase by UniqueID or LastName
@app.get("/purchases/")
def get_purchase(unique_id: str = None, lastname: str = None):
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
