import pandas as pd
import sqlalchemy as sa

import urllib
import json

from settings import DATABASE_NAME

engine = sa.create_engine(f'sqlite:///{DATABASE_NAME}')


def import_data_from_sql(table_name):
    insp = sa.inspect(engine)

    if insp.dialect.has_table(engine.connect(), table_name):
        table_data = pd.read_sql_table(table_name, engine)
    else:
        table_data = pd.DataFrame([])

    return table_data


def get_game_data_from_link(link, headers={}):

    req = urllib.request.Request(link, headers=headers)
    response = urllib.request.urlopen(req)
    url_json = json.loads(response.read().decode())

    return url_json