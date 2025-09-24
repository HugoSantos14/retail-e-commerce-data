import os
import time
from fastapi import FastAPI
from sqlalchemy import create_engine, exc
import pandas as pd

# ----- SETUP -----

def get_db_connection_string() -> str:
    user = os.getenv("POSTGRES_USER", "user")
    password = os.getenv("POSTGRES_PASSWORD", "password")
    host = os.getenv("POSTGRES_HOST", "db")
    port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "retail")
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

MAX_RETRIES = 5
RETRY_DELAY = 5
engine = None

for attempt in range(MAX_RETRIES):
    try:
        db_uri = get_db_connection_string()
        engine = create_engine(db_uri)
        
        connection = engine.connect()
        connection.close()
        print("Connected to database successfully.")
        break
    except exc.OperationalError as e:
        print(f"Attempt {attempt+1}/{MAX_RETRIES}: Failed to connect to database. Trying again in {RETRY_DELAY} seconds...")
        print(f"    (Error: {e})")
        time.sleep(RETRY_DELAY)
else:
    print("Failed to connect to database after so many attempts. The API may not run correctly.")

app = FastAPI(
    title="Retail E-Commerce API",
    description="API for serving aggregated data from gold layer to dashboard.",
    version="1.0.0"
)
    
# ----- Endpoints -----

@app.get("/")
def read_root():
    return {"message": "Welcome to the Retail API!"}

@app.get("/sales/monthly")
def get_monthly_sales():
    if engine is None:
        return {"error": "Connection with database is unavailable."}
    
    query = "SELECT * FROM monthly_sales ORDER BY invoicedate;"
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records") # DataFrame --> JSON

@app.get("/sales/by-country")
def get_sales_by_country():
    if engine is None:
        return {"error": "Connection with database is unavailable."}
    
    query = "SELECT * FROM sales_by_country;"
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

@app.get("/customers/top")
def get_top_customers():
    if engine is None:
        return {"error": "Connection with database is unavailable."}

    query = "SELECT * FROM top_customers;"
    df = pd.read_sql(query, engine)
    return df.to_dict(orient='records')

@app.get("/products/top")
def get_top_products():
    if engine is None:
        return {"error": "Connection with database is unavailable."}
        
    query = "SELECT * FROM top_products;"
    df = pd.read_sql(query, engine)
    return df.to_dict(orient='records')