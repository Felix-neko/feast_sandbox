import os, sys
from datetime import datetime

from pathlib import Path
cur_dir_path = Path(__file__).absolute().parent


if __name__ == "__main__":
    repos_path = cur_dir_path.parent / "repos"
    os.system(f"cd {str(repos_path)}/driver_parquet_repo && feast materialize-incremental {datetime.now().isoformat()}")
    os.system(f"cd {str(repos_path)}/credit_parquet_repo && feast materialize-incremental {datetime.now().isoformat()}")
    os.system(f"cd {str(repos_path)}/fraud_parquet_repo && feast materialize-incremental {datetime.now().isoformat()}")
