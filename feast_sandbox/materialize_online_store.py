import os, sys
from datetime import datetime


if __name__ == "__main__":
    os.system(f"cd ../feature_repo && feast materialize-incremental {datetime.now().isoformat()}")
    os.system(f"cd ../driver_parquet_repo && feast materialize-incremental {datetime.now().isoformat()}")