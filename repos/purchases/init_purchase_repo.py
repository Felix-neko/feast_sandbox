from pathlib import Path

from datetime import datetime, timedelta

import pandas as pd

# -----------------------------------------------------------
# 1. Create DataFrames with timestamps in both clients and purchases
# -----------------------------------------------------------

now = datetime.now()

# 1a) Clients DataFrame, now with signup_timestamp
clients_data = {
    "client_id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "email": [
        "alice@example.com",
        "bob@example.com",
        "charlie@example.com",
        "diana@example.com",
        "eve@example.com",
    ],
    "signup_channel": ["web", "mobile", "web", "email", "referral"],
    "signup_timestamp": [
        now - timedelta(days=100),
        now - timedelta(days=90),
        now - timedelta(days=80),
        now - timedelta(days=95),
        now - timedelta(days=60),
    ],
}
clients_df = pd.DataFrame(clients_data)

# Downcast timestamp to datetime64[us]
clients_df["signup_timestamp"] = pd.to_datetime(clients_df["signup_timestamp"]).astype("datetime64[us]")

# 1b) Purchases DataFrame (still with description + purchase_timestamp)
purchases_data = {
    "purchase_id": [101, 102, 103, 104, 105, 106],
    "client_id": [1, 2, 1, 4, 2, 1],
    "amount": [50.0, 23.5, 19.99, 75.0, 100.0, 5.50],
    "purchase_timestamp": [
        now - timedelta(days=5),
        now - timedelta(days=3, hours=2),
        now - timedelta(days=1, minutes=30),
        now - timedelta(days=2, hours=4),
        now - timedelta(days=1, hours=1),
        now - timedelta(hours=6),
    ],
    "description": [
        "Bought a pair of sneakers",
        "Monthly subscription fee",
        "Gift card purchase",
        "Electronics: USB-C hub",
        "Yearly software license",
        "Coffee mug and stickers",
    ],
}
purchases_df = pd.DataFrame(purchases_data)
purchases_df["purchase_timestamp"] = pd.to_datetime(purchases_df["purchase_timestamp"]).astype("datetime64[us]")

# -----------------------------------------------------------
# 2. Save both DataFrames to Parquet using FastParquet
# -----------------------------------------------------------

clients_parquet_path = Path(__file__).parent / "feature_repo/data/clients.parquet"
purchases_parquet_path = Path(__file__).parent / "feature_repo/data/purchases.parquet"

clients_df.to_parquet(clients_parquet_path, coerce_timestamps="us")
purchases_df.to_parquet(purchases_parquet_path, coerce_timestamps="us")

# -----------------------------------------------------------
# 3. Read both from disk
# -----------------------------------------------------------

clients_loaded = pd.read_parquet(clients_parquet_path)
purchases_loaded = pd.read_parquet(purchases_parquet_path)

# -----------------------------------------------------------
# 4. For each client, get the latest purchase (or none)
# -----------------------------------------------------------

# Sort and group to get latest purchase
latest_per_client = purchases_loaded.sort_values("purchase_timestamp").groupby("client_id", as_index=False).last()

# Merge with clients
merged_df = pd.merge(clients_loaded, latest_per_client, on="client_id", how="left", suffixes=("", "_latest"))

# Rename for clarity
merged_df = merged_df.rename(
    columns={
        "purchase_id": "latest_purchase_id",
        "amount": "latest_amount",
        "purchase_timestamp": "latest_purchase_timestamp",
        "description": "latest_description",
    }
)

# -----------------------------------------------------------
# 5. Display
# -----------------------------------------------------------

print("\n=== Clients with Signup + Latest Purchase Info ===")
print(merged_df)

print("\n=== Clients with NO purchases ===")
print(merged_df[merged_df["latest_purchase_id"].isna()][["client_id", "name", "signup_timestamp"]])
