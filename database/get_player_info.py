import logging
import json

import pandas as pd
import urllib
import urllib.request
import sqlite3
import sqlalchemy as sa

from tqdm import tqdm

from settings import DATABASE_NAME
from misc_functions import import_data_from_sql, get_game_data_from_link

pd.set_option('display.max_columns', None)

logging.basicConfig(level='INFO')
logger = logging.getLogger()

# Settings
player_info_cols = ['player_id', 
                    'firstName', 'lastName', 'nationality', 'birthCity',
                    'position',
                    'birthDate',
                    'birthStateProvince',
                    'height',
                    'weight', 'shootsCatches']


engine = sa.create_engine(f'sqlite:///{DATABASE_NAME}')

url_prefix = 'https://statsapi.web.nhl.com'
url_toiData_prefix = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId="
people_prefix = '/api/v1/people/'


def get_player_info(player):
    
    player_info_df = pd.DataFrame([], columns=player_info_cols)
    
    player_id = player['player_id']
    player_url = url_prefix + people_prefix + str(player_id)
    player_details_dict = get_game_data_from_link(player_url)

    player_dict = player_details_dict.get('people')
    player_info_df = player_info_df.append(player_dict[0], ignore_index=True)
    
    if len(player_dict) > 1:
        logger.warning('MORE THAN ONE PERSON!')

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
            right_index=True,
            suffixes=['_old', ''])
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
    player_w_position.rename(columns={'id': 'player_id'}, inplace=True)

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

    player_games = import_data_from_sql('game_players')
    player_info = import_data_from_sql('player_info')

    player_info_not_na = player_info[~player_info['firstName'].isna()]

    if player_games.empty :
        logger.info('No games have been processed.')
        return None
    elif player_info_not_na.empty:
        logger.info('No player info has been downloaded. Processing all players')
        players_ids = player_games[['player_id']].drop_duplicates()
    else:
        players_unique = player_games[['player_id']].drop_duplicates()
        players_ids = players_unique[~players_unique['player_id'].isin(
            player_info_not_na['player_id'])]

    # Get player data     
    for _, player in tqdm(players_ids.iterrows(), total=players_ids.shape[0]):
        player_info_updated = get_player_info(player)

        player_info_updated_cleaned = player_info_updated[~player_info_updated['firstName'].isna()]
        if not player_info_updated_cleaned.empty:
            insert_into_db(
                player_info_updated_cleaned[player_info_cols], 'player_info')


if __name__ == '__main__':
    get_player_data()
