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

url_schedule_prefix = 'https://statsapi.web.nhl.com/api/v1/schedule?season='


def get_schedule(season_id):
    schedule_url = (url_schedule_prefix + season_id)

    with urllib.request.urlopen(schedule_url) as url:
        schedule_json = json.loads(url.read().decode())

    dates = schedule_json.get('dates')
    games_df = pd.DataFrame([])
    # NOTE: I'm sure there's a better/faster way to do this. Collapsing the json?
    for date in dates:
        games = date.get('games')
        for game in games:
            game_df = pd.DataFrame.from_dict(game, orient='index').T
            games_df = games_df.append(game_df, ignore_index=True)
    return games_df


def get_games_schedules_from_seasons(df_season):
    all_schedules = pd.DataFrame([])
    for _,row in df_season.iterrows():
        season_id = row['seasonId']
        logger.info(f'==={season_id}===')

        season_schedule = get_schedule(season_id)
        logger.info(f'Number of Games:  {season_schedule.shape[0]}')
        
        season_schedule_selected = season_schedule[[
            'gamePk', 'link', 'gameType', 'season', 'gameDate']]
        # season_schedule_selected.reset_index(inplace=True)

        all_schedules = all_schedules.append(
            season_schedule_selected)
        
    return all_schedules


def get_game_schedules():

    seasons = pd.read_sql_table('seasons', engine)

    all_schedules = get_games_schedules_from_seasons(seasons)
    all_schedules.rename(columns={'gamePk': 'game_id'}, inplace=True)
    
    print(all_schedules)
    all_schedules.to_sql(
            'game_schedules', engine, if_exists='append', index=False)

