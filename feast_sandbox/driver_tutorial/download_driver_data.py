"""
Let's download driver statistics data from FEAST driver ranking tutorial and save it to driver_data.parquet
(GCP CREDENTIALS env var needed!)
"""

from feast_sandbox.utils import download_bigquery_dataframe

if __name__ == "__main__":
    download_bigquery_dataframe("feast-oss.demo_data.driver_hourly_stats", "driver_data.parquet")