import pandas as pd
from pathlib import Path
from time import time

ROOT_PATH = Path(__file__).parent.parent
BRONZE_FILE =  ROOT_PATH / "data" / "bronze" / "online_retail_II.csv"
SILVER_DIR = ROOT_PATH / "data" / "silver"
SILVER_DIR.mkdir(parents=True, exist_ok=True)
SILVER_FILE = SILVER_DIR / "online_retail_cleaned.parquet"

def read_bronze():
    print(f"[INFO] Reading raw data from {BRONZE_FILE}...")
    try:
        df = pd.read_csv(BRONZE_FILE)
    except FileNotFoundError:
        print(f"[ERROR] Bronze layer file not found.")
        exit()
    
    print(f"[INFO] Raw data loaded. Initial shape: {df.shape}")
    return df
    
def clean_bronze(df: pd.DataFrame):
    df.columns = df.columns.str.lower().str.replace(" ", "_") # Snake case
    df["invoicedate"] = pd.to_datetime(df["invoicedate"], errors="coerce")
    df.dropna(subset=["invoicedate"], inplace=True)
    print(f"[CLEAN] Standardized column names and converted dates.")
    
    df.dropna(subset=["customer_id"], inplace=True)
    df["customer_id"] = df["customer_id"].astype(int)
    print(f"[CLEAN] Removed lines without 'customer_id'. Current shape: {df.shape}")
    
    df = df[~df["invoice"].str.startswith("C", na=False)]
    print(f"[CLEAN] Removed cancelled transactions. Current shape: {df.shape}")
    
    df = df[(df["quantity"] > 0) & (df["price"] > 0)]
    print(f"[CLEAN] Removed items with invalid quantity or price. Current shape: {df.shape}")
    
def enrich_and_order_bronze(df: pd.DataFrame):
    # Enrich
    df["total_price"] = df["quantity"] * df["price"]
    
    df["invoice_year"] = df["invoicedate"].dt.year
    df["invoice_month"] = df["invoicedate"].dt.month
    df["invoice_day_of_week"] = df["invoicedate"].dt.day_of_week
    df["invoice_hour"] = df["invoicedate"].dt.hour
    
    # Order
    final_cols = [
        "invoice", "stockcode", "description", "quantity", "price", "total_price",
        "customer_id", "country", "invoicedate", "invoice_year", "invoice_month",
        "invoice_day_of_week", "invoice_hour"
    ]
    df = df[final_cols]
    
    print("[ENRICH] New columns created: 'total_price' and date components.")
    
def save_to_silver(df: pd.DataFrame):
    print(f"[INFO] Saving cleaned and enriched data in Parquet format...")
    df.to_parquet(SILVER_FILE, engine="pyarrow", index=False)

def main():
    START_TIME = time()
    
    df = read_bronze()
    clean_bronze(df)
    enrich_and_order_bronze(df)
    save_to_silver(df)
    
    END_TIME = time()
    
    print(f"\n[SUCCESS] Bronze -> Silver process finished successfully!")
    print(f"Final shape of the cleaned data: {df.shape}")
    print(f"Total execution time: {END_TIME - START_TIME:.2f} seconds.")
    print(f"File saved in: {SILVER_FILE}")
    
if __name__ == "__main__":
    main()