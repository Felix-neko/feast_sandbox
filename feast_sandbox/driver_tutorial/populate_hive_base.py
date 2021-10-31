"""
Populates given HIVE base with our dataframe ``driver_data_parquet`` (table ``driver_stat``)
"""

from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

from feast_sandbox.utils import populate_table, recreate_hive_db


if __name__ == "__main__":
    cur_dir_path = Path(__file__).absolute().parent

    recreate_hive_db("driver_tutorial")

    conn_string = 'hive://localhost:10000/driver_tutorial'
    engine = create_engine(conn_string)
    conn = engine.connect()

    in_dataframe = pd.read_parquet(cur_dir_path.parent.parent / "repos/driver_parquet_repo/driver_stats.parquet")
    populate_table(in_dataframe, "driver_stats", conn, engine)

