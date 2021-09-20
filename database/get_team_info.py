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
team_info_cols = ['id', 
                    'name', 'lastName', 'nationality', 'birthCity',
                    'position',
                    'birthDate',
                    'birthStateProvince',
                    'height',
                    'weight', 'shootsCatches']


engine = sa.create_engine(f'sqlite:///{DATABASE_NAME}')

url_prefix = 'https://statsapi.web.nhl.com'
teams_prefix = '/api/v1/teams/'

'https://statsapi.web.nhl.com/api/v1/teams/ID'


def get_team_info(team):

    team_id = team['team_id']
    team_url = url_prefix + teams_prefix + str(team_id)
    team_json = get_game_data_from_link(team_url)

    team_data_json = team_json.get('teams')[0]
    
    venue = None
    if team_data_json.get('venue') is not None:
        venue = team_data_json.get('venue').get('name')
    franchise = None
    if team_data_json.get('franchise') is not None:
        franchise = team_data_json.get('franchise').get('teamName')

    team_selected = {}
    team_selected['team_id'] = team_data_json.get('id')
    team_selected['name'] = team_data_json.get('name')
    team_selected['venue'] = venue
    team_selected['abbreviation'] = team_data_json.get('abbreviation')
    team_selected['teamName'] = team_data_json.get('teamName')
    team_selected['locationName'] = team_data_json.get('locationName')
    team_selected['firstYearOfPlay'] = team_data_json.get('firstYearOfPlay')
    team_selected['division'] = team_data_json.get('division').get('name')
    team_selected['conference'] = team_data_json.get('conference').get('name')
    team_selected['franchise'] = franchise
    team_selected['franchise_id'] = team_data_json.get('franchiseId')
    team_selected['shortName'] = team_data_json.get('shortName')
    team_selected['officialSiteUrl'] = team_data_json.get('officialSiteUrl')
    team_selected['active'] = team_data_json.get('active')


    team_info_df = pd.DataFrame().from_dict(team_selected, orient='index').T #, columns=team_info_cols)
    return team_info_df

def get_game_data_from_link(link, headers={}):

    req = urllib.request.Request(link, headers=headers)
    response = urllib.request.urlopen(req)
    url_json = json.loads(response.read().decode())

    return url_json


def insert_into_db(df, table_name, if_exists='append'):
    if df is None:
        logger.info('No data in DF')
        return ''
    if not df.empty:
        df.to_sql(
            table_name, engine, if_exists=if_exists, index=False
        )
    else:
        logger.warning(f'No data to insert into {table_name}')


def get_team_data():

    team_game_info = import_data_from_sql('team_game_info')
    team_info = import_data_from_sql('team_info')

    if team_game_info.empty:
        logger.info('No games have been processed.')
        return None
    elif team_info.empty:
        logger.info('No game info has been downloaded. Processing all players')
        team_ids = team_game_info[['team_id']].drop_duplicates()
    else:
        team_unique = team_game_info[['team_id']].drop_duplicates()
        team_ids = team_unique[~team_unique['team_id'].isin(
            team_info['team_id'])]

    # Get player data     
    for _, player in tqdm(team_ids.iterrows(), total=team_ids.shape[0]):
        team_info_updated = get_team_info(player)
        insert_into_db(team_info_updated, 'team_info')
        

if __name__ == '__main__':
    get_team_data()
