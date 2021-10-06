import pandas as pd
import sqlalchemy as sa

import requests
import zipfile

import urllib
import json
import logging

from settings import DATABASE_NAME

engine = sa.create_engine(f'sqlite:///{DATABASE_NAME}')

logging.basicConfig(level='INFO')
logger = logging.getLogger()



def import_data_from_sql(table_name):
    insp = sa.inspect(engine)

    if insp.dialect.has_table(engine.connect(), table_name):
        table_data = pd.read_sql_table(table_name, engine)
    else:
        table_data = pd.DataFrame([])

    return table_data


def get_json_data_from_link(link, headers={}):
    '''Takes a url and headers (optional), returns json'''

    req = urllib.request.Request(link, headers=headers)
    response = urllib.request.urlopen(req)
    url_json = json.loads(response.read().decode())

    return url_json


def insert_into_db(df, table_name, if_exists='append'):
    if not df.empty:
        df.to_sql(
            table_name, engine, if_exists=if_exists, index=False
        )
    else:
        logger.warning(f'No data to insert into {table_name}')


def get_csv_data_from_link(link, headers={}):
    '''Takes a url and headers(optional), returns pandas dataframe'''

    try: 
        data = pd.read_csv(link, storage_options=headers)
    except: 
        data = pd.DataFrame([])

    return data


def get_zip_data_from_link(link):
    '''Given link, returns zip file'''
    
    try:
        r = requests.get(link, stream=True)
        z = zipfile.ZipFile(io.BytesIO(r.content))
    except:
        z = None

    return z
