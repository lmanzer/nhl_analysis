import pandas as pd
import urllib.request
import json
import logging

import sqlite3
import sqlalchemy as sa

from settings import DATABASE_NAME

logging.basicConfig(level='INFO')
logger = logging.getLogger()

# Settings
engine = sa.create_engine(f'sqlite:///{DATABASE_NAME}')

url_seasons = 'https://statsapi.web.nhl.com/api/v1/seasons'


# Query the season

def get_all_seasons():
    with urllib.request.urlopen(url_seasons) as url:
        seasons_json = json.loads(url.read().decode())
        
    seasons_df = pd.DataFrame(seasons_json.get('seasons'))
    logger.info('Obtained all season data')
    return seasons_df


def create_seasons_table(df):
    df.to_sql('seasons',engine, if_exists='replace', index=False)
    logger.info('Injected Seasons table into DB')



def get_seasons():

    seasons_df = get_all_seasons()

    create_seasons_table(seasons_df)
    
