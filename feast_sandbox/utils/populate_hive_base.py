"""
Populates given HIVE base with our dataframe ``driver_data_parquet`` (table ``driver_stat``)
"""

import pandas as pd

from sqlalchemy import MetaData, select, insert, func, Table, Column, Integer, String, DateTime, Float
from sqlalchemy.engine import create_engine


metadata_obj = MetaData()
driver_stat_tbl = Table("driver_stat", metadata_obj,
                        Column('event_timestamp', DateTime),
                        Column('driver_id', Integer),
                        Column('conv_rate', Float),
                        Column('acc_rate', Float),
                        Column('avg_daily_trips', Integer),
                        Column('created_timestamp', DateTime)
                        )


def populate_hive_base(conn_string: str, drivers_dataframe: pd.DataFrame):
    engine = create_engine(conn_string)
    conn = engine.connect()
    metadata_obj.create_all(engine)
    stmt = driver_stat_tbl.insert(drivers_dataframe.to_dict(orient="records"))
    conn.execute(stmt)


if __name__ == "__main__":
    in_dataframe = pd.read_parquet("driver_data.parquet")
    in_dataframe.rename(columns={"datetime": "event_timestamp", "created": "created_timestamp"}, inplace=True)
    populate_hive_base('hive://localhost:10000/driver_tutorial', in_dataframe)

