# scripts/transform.py
import os
import pandas as pd
from datetime import datetime

STAGING_DIR = "data/staging"
OUT_CSV_DIR = "data/processed/csv"
OUT_PARQUET_DIR = "data/processed/parquet"

os.makedirs(OUT_CSV_DIR, exist_ok=True)
os.makedirs(OUT_PARQUET_DIR, exist_ok=True)

# helper
def safe_parse_date(series, fmt=None):
    return pd.to_datetime(series, errors="coerce")

def main():
    users = pd.read_csv(os.path.join(STAGING_DIR, "users.csv"))
    orders = pd.read_csv(os.path.join(STAGING_DIR, "orders.csv"))

    # --- Users cleaning ---
    # Trim strings, lowercase email, handle nulls
    users = users.astype(object).where(pd.notnull(users), None)
    users['name'] = users['name'].astype(str).str.strip()
    users['email'] = users['email'].astype(str).str.strip().str.lower()
    users['city'] = users['city'].astype(str).str.strip()
    users['signup_date'] = safe_parse_date(users.get('signup_date'))

    # Replace obvious missing values (example)
    users['email'] = users['email'].replace({'none': None, 'nan': None})

    # --- Orders cleaning ---
    orders = orders.astype(object).where(pd.notnull(orders), None)
    orders['product'] = orders['product'].astype(str).str.strip()
    # ensure numeric price
    orders['price'] = pd.to_numeric(orders['price'], errors='coerce')
    orders['order_date'] = safe_parse_date(orders.get('order_date'))
    orders['status'] = orders['status'].astype(str).str.strip().str.lower()

    # Filter: remove orders with invalid price or missing user_id or missing order_date
    orders_clean = orders[
        (orders['price'].notnull()) &
        (orders['price'] > 0) &
        (orders['user_id'].notnull()) &
        (orders['order_date'].notnull())
    ].copy()

    # Add order_month (YYYY-MM) for partitioning / aggregation
    orders_clean['order_month'] = orders_clean['order_date'].dt.to_period('M').astype(str)

    # account_age_days = difference between today and signup_date
    today = pd.Timestamp.now().normalize()
    users['account_age_days'] = (today - users['signup_date']).dt.days

    # Compute LTV: simple LTV = sum(price) per user
    ltv = orders_clean.groupby('user_id', as_index=False)['price'].sum().rename(columns={'price':'ltv'})
    users = users.merge(ltv, on='user_id', how='left')
    users['ltv'] = users['ltv'].fillna(0)

    # Write CSV outputs
    users.to_csv(os.path.join(OUT_CSV_DIR, "users_clean.csv"), index=False)
    orders_clean.to_csv(os.path.join(OUT_CSV_DIR, "orders_clean.csv"), index=False)

    # Write Parquet outputs. Partition orders by order_month
    # pandas to_parquet supports partition_cols (pyarrow required)
    try:
        users.to_parquet(os.path.join(OUT_PARQUET_DIR, "dim_users.parquet"), index=False)
        orders_clean.to_parquet(os.path.join(OUT_PARQUET_DIR, "fact_orders.parquet"), index=False, partition_cols=['order_month'])
    except Exception as e:
        print("Warning: parquet write failed. Ensure pyarrow is installed: pip install pyarrow")
        # fallback: write non-partitioned parquet
        users.to_parquet(os.path.join(OUT_PARQUET_DIR, "dim_users.parquet"), index=False)
        orders_clean.to_parquet(os.path.join(OUT_PARQUET_DIR, "fact_orders.parquet"), index=False)

    print("Transformation complete. CSVs and Parquet placed in data/processed/")

if __name__ == "__main__":
    main()

