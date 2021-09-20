import logging
import json

import pandas as pd
import urllib
import urllib.request
import sqlite3
import sqlalchemy as sa

from settings import DATABASE_NAME
from misc_functions import import_data_from_sql

pd.set_option('display.max_columns', None)

logging.basicConfig(level='INFO')
logger = logging.getLogger()

# Settings
player_info_cols = ['id', 
                    'firstName', 'lastName', 'nationality', 'birthCity',
                    'primaryPosition.abbreviation',
                    'birthDate',
                    'birthStateProvince',
                    'height',
                    'weight', 'shootsCatches']


engine = sa.create_engine(f'sqlite:///{DATABASE_NAME}')

url_prefix = 'https://statsapi.web.nhl.com'
url_toiData_prefix = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId="
people_prefix = '/api/v1/people/'
people_suffix = '/stats'

url_prefix + people_prefix + player_id + people_suffix


def get_player_info(game_data):
    player_dict = game_data.get('players')

    player_info_df = pd.DataFrame.from_dict(player_dict).T

    # Extract useful primary position
    if 'primaryPosition' in player_info_df.columns:

        player_position_dict = player_info_df[['primaryPosition']].to_dict(orient='index')

        primary_position_dict = [[id, data.get('primaryPosition').get('abbreviation') ]
         for id, data in player_position_dict.items()]
    
        position_df = pd.DataFrame(
            primary_position_dict, columns=['index', 'position'])
        position_df.set_index('index', inplace=True)

        player_w_position = pd.merge(player_info_df, 
            position_df, 
            left_index=True,
            right_index=True)
    else:
        player_w_position = player_info_df.copy()
        player_w_position['position'] = None
        
    # Remove remaining dicts from dataframe
    if 'currentTeam' in player_w_position.columns:
        player_w_position.drop(
            ['currentTeam'], 
            axis=1,
            inplace=True)
    if 'primaryPosition' in player_w_position.columns:
        player_w_position.drop(
            [ 'primaryPosition'],
            axis=1,
            inplace=True)
        
    return player_w_position


def get_game_data_from_link(link, headers={}):

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
                    

 def get_player_data():

    player_games = import_data_from_sql('player_games')
    player_info = import_data_from_sql('player_info')

    # if player_games.empty :
    #     missing_game_data = game_schedules

    # # Get player data 
    player_info = get_player_info(game_data)

    # insert_into_db(player_info, 'player_info')


if __name__ == '__main__':
    get_player_data()
