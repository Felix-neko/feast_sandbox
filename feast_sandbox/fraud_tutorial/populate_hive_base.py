import logging
from sqlalchemy import create_engine
import pandas as pd

from pathlib import Path

from feast_sandbox.utils import populate_table


if __name__ == "__main__":
    cur_dir_path = Path(__file__).absolute().parent

    parquet_paths = [elm for elm in (cur_dir_path.parent.parent / "repos/fraud_parquet_repo").iterdir()
                     if elm.is_file() and str(elm).endswith(".parquet")]

    conn_string = 'hive://localhost:10000/fraud_tutorial2'
    engine = create_engine(conn_string)
    conn = engine.connect()

    for ppath in parquet_paths:
        table_name = ppath.name.split(".")[0]
        df = pd.read_parquet(ppath)
        populate_table(df, table_name, conn, engine)
