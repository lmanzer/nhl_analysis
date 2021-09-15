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
    seasons_df.to_sql('seasons',engine, if_exists='replace')
    logger.info('Injected Seasons table into DB')


def add_monitoring_columns(df_raw):
    df = df_raw.copy()
    df['processed_games'] = 0
    df['processed_all_games'] = 0
    df['in_progress'] = 0

    return df

if __name__ == '__main__':
    seasons_raw_df = get_all_seasons()

    seasons_df = add_monitoring_columns(seasons_raw_df)

    create_seasons_table(seasons_df)
    
    test = pd.read_sql_table('seasons', engine)
    print(test.head())
