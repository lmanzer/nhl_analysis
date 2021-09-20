import pandas as pd
import sqlalchemy as sa

from settings import DATABASE_NAME

engine = sa.create_engine(f'sqlite:///{DATABASE_NAME}')


def import_data_from_sql(table_name):
    insp = sa.inspect(engine)

    if insp.dialect.has_table(engine.connect(), table_name):
        table_data = pd.read_sql_table(table_name, engine)
    else:
        table_data = pd.DataFrame([])

    return table_data
