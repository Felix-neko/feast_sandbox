import sqlite3
import pandas as pd

if __name__ == "__main__":
    con = sqlite3.connect("../repos/feature_repo/data/online_store.db")
    print("\n--- Schema of online store ---")
    print(
        pd.read_sql_query(
            "SELECT * FROM feature_repo_driver_hourly_stats", con).columns.tolist())
    con.close()