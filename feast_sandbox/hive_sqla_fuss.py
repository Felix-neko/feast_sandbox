from sqlalchemy import select, func
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import MetaData, Table

from sqlalchemy import insert, select

from sqlalchemy.ext.automap import automap_base

if __name__ == "__main__":
    Base = automap_base()

    engine = create_engine('hive://localhost:10000/hell_db')
    conn = engine.connect();
    logs = Table('daemon', MetaData(bind=engine), autoload=True)
    print(select([func.count('*')], from_obj=logs).scalar())

    Base.prepare(engine, reflect=True)
    daemon_tbl = Base.metadata.tables['daemon']

    # stmt = insert(daemon_tbl).values(id=1, name="Leviathan", rank=30)
    # print(stmt)
    # compiled = stmt.compile()
    # conn.execute(stmt)

    # print(daemon_tbl)

    stmt = select(daemon_tbl.c.id, daemon_tbl.c.name).where(daemon_tbl.c.name == "Leviathan")
    print(stmt)

    print(conn.execute(stmt).all())