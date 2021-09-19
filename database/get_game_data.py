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
skater_stats_cols = ["player_id","timeOnIce", "assists", "goals", "shots", "hits", "powerPlayGoals",
                     "powerPlayAssists", "penaltyMinutes", "faceOffWins", "faceoffTaken",
                     "takeaways",  "giveaways", "shortHandedGoals", "shortHandedAssists",
                     "blocked",
                     "plusMinus", "evenTimeOnIce", "shortHandedTimeOnIce",
                     "powerPlayTimeOnIce", 'test_col']
goalie_stats_cols = ["player_id", "timeOnIce", "assists", "goals", "shots",
                     "saves", "pim", "powerPlaySaves", "shortHandedSaves",
                     "evenSaves", "shortHandedShotsAgainst",
                     "evenShotsAgainst", "powerPlayShotsAgainst", "decision",
                     "savePercentage",  "powerPlaySavePercentage",
                     "evenStrengthSavePercentage"]

player_info_cols = ['id', 
                    'firstName', 'lastName', 'nationality', 'birthCity',
                    'primaryPosition.abbreviation',
                    'birthDate',
                    'birthStateProvince',
                    'height',
                    'weight', 'shootsCatches']


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

def get_game_overview(game_id, game_data, live_data, venue_data):
    game_properties = {}
    # Get Game Properties
    game_properties['gamePk'] = game_id
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
    game_properties['venue'] = (
        'Unknown' if venue_data == None else venue_data.get('name'))
    game_properties['outcome'] = determine_outcome(live_data)

    return game_properties


def get_player_stats_by_team(game_dict, is_home):
    skater_stats_all_df = pd.DataFrame([], columns=skater_stats_cols)
    goalie_stats_all_df = pd.DataFrame([], columns=goalie_stats_cols)
    _scratches_stats_df = pd.DataFrame([], columns=['player_id'])

    # Get Skater and Goalie Properties
    player_dict = game_dict.get('players')
    for index, player in player_dict.items():
        player_id = player.get('person').get('id')

        # Check if player played in game
        if (player.get('stats') is not None) & (player.get('stats') != {}):
            player_stats = player.get('stats')
            skater_stats = player_stats.get('skaterStats')
            goalie_stats = player_stats.get('goalieStats')
            # Check whether goalie or skater (or other?)
            if skater_stats is not None:
                skater_stats_all_df = skater_stats_all_df.append(
                    skater_stats, ignore_index=True)
                skater_stats_all_df['player_id'] = player_id
            elif goalie_stats is not None:
                goalie_stats_all_df = goalie_stats_all_df.append(
                    goalie_stats, ignore_index=True)
                goalie_stats_all_df['player_id'] = player_id
            else:
                other_player = 'test'
                print('--------Not goalie or skater -------')
                # print(player_stats)
    _skater_stats_df = skater_stats_all_df[skater_stats_cols]
    _skater_stats_df['is_home'] = is_home

    _goalie_stats_df = goalie_stats_all_df[goalie_stats_cols]
    _goalie_stats_df['is_home'] = is_home

    # Get Scratches Properties
    scratches_dict = game_dict.get('scratches')
    if len(scratches_dict)>0:
        _scratches_stats_df = pd.DataFrame(scratches_dict, columns=['player_id'])

    return _skater_stats_df, _goalie_stats_df, _scratches_stats_df


def get_player_stats(live_data, game_data, game_id):

    # Home Stats
    home_team_id = game_data.get('teams').get('home').get('id')
    home_game_dict = live_data.get('boxscore').get('teams').get('home')

    home_skater_stats_df, home_goalie_stats_df, home_scratches_stats_df = get_player_stats_by_team(
        home_game_dict, is_home=1)
    home_goalie_stats_df['team_id'] = home_team_id
    home_goalie_stats_df['game_id'] = game_id
    
    home_skater_stats_df['team_id'] = home_team_id
    home_skater_stats_df['game_id'] = game_id

    home_scratches_stats_df['team_id'] = home_team_id
    home_scratches_stats_df['game_id'] = game_id

    # Away Stats
    away_team_id = game_data.get('teams').get('away').get('id')
    away_game_dict = live_data.get('boxscore').get('teams').get('away')

    away_skater_stats_df, away_goalie_stats_df, away_scratches_stats_df = get_player_stats_by_team(
        away_game_dict, is_home=0)

    away_goalie_stats_df['team_id'] = away_team_id
    away_goalie_stats_df['game_id'] = game_id
    
    away_skater_stats_df['team_id'] = away_team_id
    away_skater_stats_df['game_id'] = game_id

    away_scratches_stats_df['team_id'] = away_team_id
    away_scratches_stats_df['game_id'] = game_id

    # Combine home and away
    skater_stats_df = pd.concat([away_skater_stats_df, home_skater_stats_df])
    goalie_stats_df = pd.concat([away_goalie_stats_df, home_goalie_stats_df])
    scratches_stats_df = pd.concat(
        [away_scratches_stats_df, home_scratches_stats_df])

    return skater_stats_df, goalie_stats_df, scratches_stats_df


def player_info(game_data):
    player_dict = game_data.get('players')

    player_info_df = pd.DataFrame.from_dict(player_dict).T

    player_position_dict = player_info_df[['primaryPosition']].to_dict(orient='index')

    test = [[id, data.get('primaryPosition').get('abbreviation') ]
    for id, data in player_position_dict.items()]
    
    position_df = pd.DataFrame(test, columns=['index', 'position'])
    position_df.set_index('index', inplace=True)
    print(position_df)

    player_w_position = pd.merge(player_info_df, 
        position_df, 
        left_index=True,
        right_index=True)
        
    return player_w_position


def get_team_info_by_home_away(HoA, game_id, game_data, live_data, venue_data):
    print(game_id)

    team_data = game_data.get('teams').get(HoA)
    team_stats = live_data.get('boxscore').get('teams').get(HoA)
    team_coaches = team_stats.get(
        'coaches')
    
    if len(team_coaches) == 0:
        head_coach = ''
    else: 
        head_coach = team_stats.get(
            'coaches')[0].get('person').get('fullName')

    print('---')
    team_properties = {}
    team_properties['game_id'] = game_id
    team_properties['team_id'] = team_data.get('id')
    team_properties['HoA'] = HoA
    # Need to figure this out: func_helper_get_winner & func_helper_get_settled_in
    team_properties['winner'] = 'TBD'
    team_properties['settled_in'] = 'TBD'
    team_properties['head_coach'] = head_coach
    team_properties['goals'] = team_stats.get(
        'teamStats').get('teamSkaterStats').get('goals')
    team_properties['shots'] = team_stats.get(
        'teamStats').get('teamSkaterStats').get('shots')
    team_properties['hits'] = team_stats.get(
        'teamStats').get('teamSkaterStats').get('hits')
    team_properties['pim'] = team_stats.get(
        'teamStats').get('teamSkaterStats').get('pim')
    team_properties['powerPlayOpportunities'] = team_stats.get(
        'teamStats').get('teamSkaterStats').get('powerPlayOpportunities')
    team_properties['powerPlayGoals'] = team_stats.get(
        'teamStats').get('teamSkaterStats').get('powerPlayGoals')
    team_properties['faceOffWinPercentage'] = team_stats.get(
        'teamStats').get('teamSkaterStats').get('faceOffWinPercentage')
    team_properties['giveaways'] = team_stats.get(
        'teamStats').get('teamSkaterStats').get('giveaways')
    team_properties['takeaways'] = team_stats.get(
        'teamStats').get('teamSkaterStats').get('takeaways')
    team_properties['blocked'] = team_stats.get(
        'teamStats').get('teamSkaterStats').get('blocked')
    team_properties['startRinkSide'] = live_data.get('linescore').get(
        'periods')[0].get(HoA).get('rinkSide')

    return team_properties


def get_team_info(game_id, game_data, live_data, venue_data):
    home_properties = get_team_info_by_home_away(
        'home', game_id, game_data, live_data, venue_data)
    away_properties = get_team_info_by_home_away(
        'away', game_id, game_data, live_data, venue_data)

    HoA_team_properties = pd.DataFrame().from_dict(home_properties, orient='index').T
    HoA_team_properties = HoA_team_properties.append(away_properties, ignore_index=True)
    
    return HoA_team_properties





def get_game_data(games_df):

    all_game_overview = pd.DataFrame()
    skater_stats_all = pd.DataFrame()
    goalie_stats_all = pd.DataFrame()
    for _, game in games_df.iterrows():
        game_link = url_prefix + game['link']
        with urllib.request.urlopen(game_link) as url:
            game_json = json.loads(url.read().decode())

        game_id = game['gamePk']
        game_data = game_json.get('gameData')
        live_data = game_json.get('liveData')
        venue_data = game_data.get('venue')

        # Get player data
        # player_info = player_info(game_data)

        # Get Game Overview
        # game_overview = get_game_overview(
        #     game_id, game_data, live_data, venue_data)
        # all_game_overview = all_game_overview.append(
        #     game_overview, ignore_index=True)
        # all_game_overview.to_sql(
        #     'games', engine, if_exists='replace')

        # Get Team Info
        get_team_info(game_id, game_data, live_data, venue_data)


        # Get Player-Game Stats
        # skater_stats_df, goalie_stats_df, scratches_stats_df = get_player_stats(
        #     live_data, game_data, game_id)

        # skater_stats_all = skater_stats_all.append(
        #     skater_stats_df, ignore_index=True)
        # skater_stats_all.to_sql(
        #     'skater_game_stats', engine, if_exists='replace')

        # goalie_stats_all = goalie_stats_all.append(
        #     goalie_stats_df, ignore_index=True)
        # goalie_stats_all.to_sql(
        #     'goalie_game_stats', engine, if_exists='replace')
        break
        


if __name__ == '__main__':
    # TODO: potentially use pandas to take single json and break into readable format rather than bits and pieces
    games = pd.read_sql_table('game_schedules', engine)

    games_test = games[games['gamePk']==1967020088]
    print(games_test)

    get_game_data(games_test)
    print('---------------------')
 
