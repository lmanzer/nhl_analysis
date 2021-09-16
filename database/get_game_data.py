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

def determine_outcome(data_json):
    # TODO: What the last period is like when the result is settled in shootout
    home_goals = data_json.get('linescore').get(
        'teams').get('home').get('goals')
    away_goals = data_json.get('linescore').get(
        'teams').get('away').get('goals')
 
    last_period = data_json.get('linescore').get('currentPeriod')

    if home_goals > away_goals:
        winner = 'home'
    elif away_goals > home_goals:
        winner = 'away'
    else:
        winner = 'tie'
    
    if last_period > 3:
        period = 'OT/Shootout'
        # Add  shootout here if possible
    elif last_period == 3:
        period = 'REG'
    else:
        period = 'TBC'

    return winner + ' win ' + period + str(last_period)


def get_game_data(df):
    all_game_data = pd.DataFrame()
    for _, game in df.iterrows():
        print('====')
        game_link = url_prefix + game['link']
        with urllib.request.urlopen(game_link) as url:
            game_json = json.loads(url.read().decode())
        game_data = game_json.get('gameData')
        live_data = game_json.get('liveData')
        venue_data = game_data.get('venue')

        game_properties = {}
        # Get Game Properties
        game_properties['game_id'] = game.get('gamePk')
        game_properties['season'] = game_data.get('game').get('season')
        game_properties['type'] = game_data.get('game').get('type')
        game_properties['date_time_GMT'] = game_data.get(
            'datetime').get('dateTime')
        game_properties['away_team_id'] = game_data.get(
            'teams').get('away').get('id')
        game_properties['home_team_id'] = game_data.get(
            'teams').get('home').get('id')
        game_properties['away_goals'] = live_data.get(
            'linescore').get('teams').get('away').get('goals')
        game_properties['home_goals'] = live_data.get(
            'linescore').get('teams').get('home').get('goals')

        # TODO: Determine why there are missing  Venues
        game_properties['venue'] = ('Unknown' if venue_data == None else venue_data.get('name'))
        game_properties['outcome'] = determine_outcome(live_data)

        all_game_data = all_game_data.append(game_properties, ignore_index=True)
    print(all_game_data)
    return all_game_data
        


if __name__ == '__main__':
    games = pd.read_sql_table('game_schedules', engine)

    get_game_data(games.sample(10))
