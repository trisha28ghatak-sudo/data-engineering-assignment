# scripts/ingest.py
import os
import shutil

SRC_USERS = "data/raw/users.csv"
SRC_ORDERS = "data/raw/orders.csv"
STAGING_DIR = "data/staging"

os.makedirs(STAGING_DIR, exist_ok=True)

def ingest_file(src, dest_dir):
    if not os.path.exists(src):
        raise FileNotFoundError(f"{src} not found. Please put it in data/raw/")
    dest = os.path.join(dest_dir, os.path.basename(src))
    # simple copy (keeps raw file unchanged)
    shutil.copy(src, dest)
    print(f"Copied {src} -> {dest}")

def main():
    ingest_file(SRC_USERS, STAGING_DIR)
    ingest_file(SRC_ORDERS, STAGING_DIR)
    print("Ingestion complete. Staging files are in data/staging/")

if __name__ == "__main__":
    main()
