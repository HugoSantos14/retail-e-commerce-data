import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from time import time

def get_db_connection_string() -> str:
    user = os.getenv("POSTGRES_USER", "user")
    password = os.getenv("POSTGRES_PASSWORD", "password")
    host = os.getenv("POSTGRES_HOST", "retail_db")
    port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "retail")
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

def create_monthly_sales(df: pd.DataFrame) -> pd.DataFrame:
    print("  -> Generating report: Monthly sales...")
    monthly_sales = df.set_index("invoicedate").resample("MS").agg(
        total_revenue=("total_price", "sum"),
        total_transactions=("invoice", "nunique"),
        active_customers=("customer_id", "nunique")
    ).reset_index()
    monthly_sales["invoice_month"] = monthly_sales["invoicedate"].dt.to_period("M")
    monthly_sales["invoice_month"] = monthly_sales["invoice_month"].astype(str)
    return monthly_sales

def create_sales_by_country(df: pd.DataFrame) -> pd.DataFrame:
    print("  -> Generating report: Sales by country...")
    sales_by_country = df.groupby("country").agg(
        total_revenue=("total_price", "sum"),
        total_transactions=("invoice", "nunique")
    ).reset_index().sort_values(by="total_revenue", ascending=False)
    return sales_by_country

def create_top_customers(df: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    print(f"  -> Generating report: Top {n} customers...")
    top_customers = df.groupby("customer_id").agg(
        total_revenue=("total_price", "sum"),
        total_transactions=("invoice", "nunique")
    ).reset_index().sort_values(by="total_revenue", ascending=False).head(n)
    return top_customers

def create_top_products(df: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    print(f"  -> Generating report: Top {n} products...")
    top_products = df.groupby(["stockcode", "description"]).agg(
        units_sold=("quantity", "sum"),
        total_revenue=("total_price", "sum")
    ).reset_index().sort_values(by="units_sold", ascending=False).head(n)
    return top_products

def load_to_postgres(df: pd.DataFrame, table_name: str, engine):
    print(f"    -> Loading table '{table_name}' to PostgreSQL...")
    df.to_sql(
        table_name,
        con=engine,
        if_exists="replace",
        index=False
    )

def main():
    START_TIME = time()
    
    ROOT_PATH = Path(__file__).parent.parent
    SILVER_FILE = ROOT_PATH / "data" / "silver" / "online_retail_cleaned.parquet"

    try:
        print(f"[INFO] Reading cleaned data from silver layer: {SILVER_FILE}")
        df_silver = pd.read_parquet(SILVER_FILE)
        
        print("\n[INFO] Starting the table creation of gold layer...")
        df_monthly_sales = create_monthly_sales(df_silver)
        df_sales_by_country = create_sales_by_country(df_silver)
        df_top_customers = create_top_customers(df_silver)
        df_top_products = create_top_products(df_silver)
        
        print("\n[INFO] Connecting to the PostgreSQL database...")
        engine = create_engine(get_db_connection_string())
        
        load_to_postgres(df_monthly_sales, "monthly_sales", engine)
        load_to_postgres(df_sales_by_country, "sales_by_country", engine)
        load_to_postgres(df_top_customers, "top_customers", engine)
        load_to_postgres(df_top_products, "top_products", engine)

        END_TIME = time()
        print("\n[SUCCESS] Process Silver -> Gold finished successfully!")
        print(f"Gold layer tables loaded to database.")
        print(f"Total execution time: {END_TIME - START_TIME:.2f} seconds.")

    except FileNotFoundError:
        print(f"[ERROR] Silver layer file not found.")
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()