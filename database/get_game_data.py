import logging
import json

import pandas as pd
import urllib
import urllib.request
import sqlite3
import sqlalchemy as sa

from settings import DATABASE_NAME

pd.set_option('display.max_columns', None)

logging.basicConfig(level='INFO')
logger = logging.getLogger()

# Settings
engine = sa.create_engine(f'sqlite:///{DATABASE_NAME}')


url_prefix = 'https://statsapi.web.nhl.com'


def get_game_data(df):
    for _, game in df.iterrows():
        game_link = url_prefix + game['link']
        with urllib.request.urlopen(game_link) as url:
            game_json = json.loads(url.read().decode())
        print(game_json)


if __name__ == '__main__':
    games = pd.read_sql_table('games', engine)

    get_game_data(games)
