"""
Populates given HIVE base with our dataframe ``driver_data_parquet`` (table ``driver_stat``)
"""

from pathlib import Path

import numpy as np
import pandas as pd


from sqlalchemy import MetaData, select, insert, func, Table, Column, Integer, String, DateTime, Float
from sqlalchemy.engine import create_engine, Engine, Connection

import logging

cur_dir_path = Path(__file__).absolute().parent

metadata_obj = MetaData()

pd_to_sqla_dtypes = {np.dtype("int64"): Integer, np.dtype('O'): String, np.datetime64: DateTime,
                     np.dtype("float64"): Float, np.dtype('<M8[ns]'): DateTime}

CHUNK_SIZE = 10000


def populate_table(df: pd.DataFrame, table_name: str, conn: Connection, engine: Engine):

    sqla_cols = []
    for col, dtype in zip(df.columns, df.dtypes):
        if isinstance(dtype, pd.DatetimeTZDtype):
            sqla_cols.append(Column(col, DateTime))
        elif np.issubdtype(dtype, np.datetime64):
            sqla_cols.append(Column(col, DateTime))
        else:
            sqla_cols.append(Column(col, pd_to_sqla_dtypes[dtype]))

    table = Table(table_name, metadata_obj, *sqla_cols)
    metadata_obj.create_all(engine)

    print(f"Populating table {table_name}")
    for start_idx in range(0, df.shape[0], CHUNK_SIZE):
        stmt = table.insert().values(df.iloc[start_idx:(start_idx + CHUNK_SIZE)].to_dict("records"))
        conn.execute(stmt)
        print(f"Inserted {min(start_idx + CHUNK_SIZE, df.shape[0])} / {df.shape[0]} rows")
    print("Table complete")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    credit_history_df = pd.read_parquet(str(cur_dir_path.parent.parent / "credit_parquet_repo/credit_history.parquet"))
    zipcode_table_df = pd.read_parquet(str(cur_dir_path.parent.parent / "credit_parquet_repo/zipcode_table.parquet"))

    conn_string = 'hive://localhost:10000/credit_tutorial'
    engine = create_engine(conn_string)
    conn = engine.connect()

    populate_table(zipcode_table_df, "zipcode_data2", conn, engine)
    populate_table(credit_history_df, "credit_history2", conn, engine)

