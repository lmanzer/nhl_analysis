import io
import os
import logging
import json
import datetime

import pandas as pd
import urllib
import urllib.request
import sqlite3
import sqlalchemy as sa


from tqdm import tqdm

from settings import DATABASE_NAME
from misc_functions import (
    import_data_from_sql, 
    get_csv_data_from_link, 
    insert_into_db,
    get_zip_data_from_link)

SHOT_DATA_URL = 'https://peter-tanner.com/moneypuck/downloads/'
MONEYPUCK_URL = 'https://moneypuck.com/moneypuck/playerData/seasonSummary/'

game_types = ['regular', 'playoffs']
data_types = ['lines', 'skaters', 'goalies', 'teams']
'playerData/playerGameByGame/{year}/{type}/{game}.csv'


DATA_PATH = os.path.join('data')
toi_hdr = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
}

    
def get_season_summary_data():

    today = datetime.datetime.today().year
    years = range(2007, today+1)
    for data_type in data_types:
        data_type_df = pd.DataFrame([])
        for year in years:
            for game_type in game_types:
                data_url = MONEYPUCK_URL + \
                    f'{year}/{game_type}/{data_type}.csv'
                headers = {'User-Agent': 'Mozilla/5.0'}
                data = get_csv_data_from_link(data_url, headers)
                if not data.empty:
                    data['game_type'] = game_type
                    data['year'] = year
                    
                    data_type_df = data_type_df.append(data)
                
        insert_into_db(
            data_type_df, f'{data_type}_advanced', if_exists='replace')


def get_shot_data():
    today = datetime.datetime.today().year
    years = range(2007, today + 1)
    MONEYPUCK_PATH = os.path.join(DATA_PATH, 'moneypuck')
    os.makedirs(MONEYPUCK_PATH, exist_ok=True)

    # Extract Data
    for year in years:
        shot_year_url = SHOT_DATA_URL + f'shots_{year}.zip'

        zips = get_zip_data_from_link(shot_year_url)
        if zips is not None:
            zips.extractall(MONEYPUCK_PATH)

    # Load Data
    yearly_shot_data = pd.DataFrame([])
    for file in os.listdir(MONEYPUCK_PATH):
        shot_path = os.path.join(MONEYPUCK_PATH, file)
        shot_data = pd.read_csv(shot_path)
        yearly_shot_data = yearly_shot_data.append(shot_data)

    insert_into_db(
        yearly_shot_data, f'shot_data_advanced', if_exists='replace')


def get_moneypuck_data():

    get_season_summary_data()
    get_shot_data()

