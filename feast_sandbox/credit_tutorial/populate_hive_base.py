import logging
from sqlalchemy import create_engine
import pandas as pd

from pathlib import Path

from feast_sandbox.utils import populate_table




if __name__ == "__main__":
    cur_dir_path = Path(__file__).absolute().parent
    logging.getLogger().setLevel(logging.INFO)

    credit_history_df = pd.read_parquet(str(cur_dir_path.parent.parent / "credit_parquet_repo/credit_history.parquet"))
    zipcode_table_df = pd.read_parquet(str(cur_dir_path.parent.parent / "credit_parquet_repo/zipcode_table.parquet"))

    conn_string = 'hive://localhost:10000/credit_tutorial'
    engine = create_engine(conn_string)
    conn = engine.connect()

    populate_table(zipcode_table_df, "zipcode_data", conn, engine)
    populate_table(credit_history_df, "credit_history", conn, engine)

