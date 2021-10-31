"""
Let's download driver statistics data from FEAST driver ranking tutorial and save it to driver_data.parquet
(GCP CREDENTIALS env var needed!)
"""

from pathlib import Path

from feast_sandbox.utils import download_data

cur_dir_path = Path(__file__).absolute().parent

if __name__ == "__main__":
    download_data("feast-oss.demo_data.driver_hourly_stats",
                  cur_dir_path.parent.parent / "repos/driver_parquet_repo/driver_stats.parquet",
                  {"datetime": "event_timestamp", "created": "created_timestamp"})