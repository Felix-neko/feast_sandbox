import os, sys
from datetime import datetime


if __name__ == "__main__":
    os.system(f"cd ../feature_repo && feast materialize-incremental {datetime.now().isoformat()}")