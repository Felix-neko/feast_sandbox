from typing import Optional, Dict

from pathlib import Path

import numpy as np
import pandas as pd

from google.cloud import bigquery
from sqlalchemy import MetaData, select, insert, func, Table, Column, Integer, String, DateTime, Float
from sqlalchemy.engine import create_engine, Engine, Connection

cur_dir_path = Path(__file__).absolute().parent

metadata_obj = MetaData()
pd_to_sqla_dtypes = {np.dtype("int64"): Integer, np.dtype('O'): String, np.datetime64: DateTime,
                     np.dtype("float64"): Float, np.dtype('<M8[ns]'): DateTime}
CHUNK_SIZE = 10000


def populate_table(df: pd.DataFrame, table_name: str, conn: Connection, engine: Engine):
    """
    Populates given HIVE base with our dataframe ``driver_data_parquet`` (table ``driver_stat``)
    """
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


def download_bigquery_dataframe(table_name: str, dump_path: Optional[str] = None,
                                field_name_map: Optional[Dict[str, str]] = None) -> pd.DataFrame :
    bqclient = bigquery.Client()
    # Download query results.
    query_string = f"SELECT * FROM {table_name}"
    dataframe = bqclient.query(query_string).result().to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )

    if field_name_map is not None:
        dataframe.rename(columns=field_name_map, inplace=True)
    if dump_path is not None:
        with open(dump_path, "wb") as out_file:
            out_file.write(dataframe.to_parquet())

    return dataframe