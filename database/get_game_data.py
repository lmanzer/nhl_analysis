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



engine = sa.create_engine(f'sqlite:///{DATABASE_NAME}')

url_prefix = 'https://statsapi.web.nhl.com'
url_toiData_prefix = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId="

toi_hdr = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
}


def get_game_winner(live_data):
    home_goals = live_data.get('linescore').get(
        'teams').get('home').get('goals')
    away_goals = live_data.get('linescore').get(
        'teams').get('away').get('goals')

    last_period = live_data.get('linescore').get('currentPeriod')

    if home_goals > away_goals:
        winner = 'home'
    elif away_goals > home_goals:
        winner = 'away'
    else:
        winner = 'tie'

    return winner

def game_settled_in(live_data):
    last_period = live_data.get('linescore').get('currentPeriod')
    if last_period > 3:
        period = 'OT/Shootout'
        # Add  shootout here if possible
    elif last_period == 3:
        period = 'REG'
    else:
        period = 'TBC'
    
    return period


def determine_outcome(live_data):
    # TODO: What the last period is like when the result is settled in shootout
    winner = get_game_winner(live_data)
    
    last_period = live_data.get('linescore').get('currentPeriod')
    period = game_settled_in(live_data)

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

    game_properties_df = pd.DataFrame().from_dict(game_properties, orient='index').T
    return game_properties_df


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


def get_team_info_by_home_away(HoA, game_id, game_data, live_data, venue_data):

    team_data = game_data.get('teams').get(HoA)
    team_stats = live_data.get('boxscore').get('teams').get(HoA)
    team_coaches = team_stats.get(
        'coaches')
    
    if len(team_coaches) == 0:
        head_coach = ''
    else: 
        head_coach = team_stats.get(
            'coaches')[0].get('person').get('fullName')
    team_properties = {}
    team_properties['game_id'] = game_id
    team_properties['team_id'] = team_data.get('id')
    team_properties['HoA'] = HoA
    team_properties['winner'] = get_game_winner(live_data)
    team_properties['settled_in'] = game_settled_in(live_data)
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


def get_game_plays(game_id, live_data, game_data):
    game_plays = []
    
    allplays = live_data.get('plays').get('allPlays')

    home_team_id = game_data.get(
        'teams').get('home').get('id')
    away_team_id = game_data.get(
        'teams').get('away').get('id')
 
    for plays in allplays:
        team = plays.get('team')
        if team is None:
            team_id = None
        else:
            team_id = team.get('id')

        team_against_id = None
        if team_id == home_team_id:
            team_against_id = away_team_id
        elif team_id == away_team_id:
            team_against_id = home_team_id

        strength = None
        if plays.get('strength') is not None:
            strength = plays.get('strength')

        goals = plays.get('goals')
        home_goals = None
        away_goals = None
        if  goals is not None:
            home_goals = goals.get('home')
            away_goals = goals.get('away')

        gameWinningGoal = None
        if plays.get('gameWinningGoal') is not None:
            gameWinningGoal = plays.get('gameWinningGoal')

        _play_data = {}
        _play_data['play_id'] = str(game_id) + '_' + str(plays.get('about').get('eventId'))
        _play_data['game_id'] = game_id 
        _play_data['event_id'] = plays.get('about').get('eventId')
        _play_data['team_id'] = team_id
        _play_data['team_id_against'] = team_against_id 
        _play_data['event'] = plays.get('result').get('event')
        _play_data['secondaryType'] = plays.get('result').get('secondaryType')
        
        #some vars not in all event types:
        _play_data['strength'] = strength
        _play_data['gameWinningGoal'] = plays.get('gameWinningGoal')
        _play_data['emptyNet'] = plays.get('emptyNet')
        _play_data['penaltySeverity'] = plays.get('penaltySeverity')
        _play_data['penaltyMinutes'] = plays.get('penaltyMinutes')

        _play_data['x'] = plays.get('x')
        _play_data['y'] = plays.get('y')
        _play_data['period'] = plays.get('period')
        _play_data['periodType'] = plays.get('periodType')
        _play_data['periodTime'] = plays.get('periodTime')
        _play_data['periodTimeRemaining'] = plays.get('periodTimeRemaining')
        _play_data['dateTime'] = plays.get('dateTime')

        _play_data['goals_home'] = home_goals
        _play_data['goals_away'] = away_goals
        
        _play_data['description'] = plays.get('description')

        game_plays.append(_play_data)

    game_plays_df = pd.DataFrame(game_plays)

    return game_plays_df


def get_game_plays_players(game_id, live_data, game_data):
    allplays = live_data.get('plays').get('allPlays')

    home_team_id = game_data.get(
        'teams').get('home').get('id')
    away_team_id = game_data.get(
        'teams').get('away').get('id')

    game_play_players = []
    for plays in allplays:
        players = plays.get('players')
        if players is None:
            pass
        else:
            for player in players:
                _game_play_player = {}
                _game_play_player['play_id'] = str(
                    game_id) + '_' + str(plays.get('about').get('eventId'))
                _game_play_player['game_id'] = game_id
                _game_play_player['player_id'] = player.get('player').get('id')
                _game_play_player['player_type'] = player.get('playerType')
                game_play_players.append(_game_play_player)
    
    game_play_players = pd.DataFrame(game_play_players)
    return game_play_players


def get_shift_data(toi_json):
    shift_data = []
    for player in toi_json.get('data'):
        player_shift_data = {}

        player_shift_data['game_id'] = player.get('gameId')
        player_shift_data['player_id'] = player.get('playerId')
        player_shift_data['period'] = player.get('period')
        player_shift_data['shift_start'] = player.get('startTime')
        player_shift_data['shift_end'] = player.get('endTime')
        shift_data.append(player_shift_data)

    shift_data_df = pd.DataFrame(shift_data)
    return shift_data_df


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


def get_game_data(games_df):
    '''Description: For each game in game schedules, obtain all information
    about the game for each team and player. Also grab all player information. '''

    for _, game in games_df.iterrows():
 
        # Get data from websites
        toi_json = get_game_data_from_link(
            url_toiData_prefix + str(game['gamePk']), headers=toi_hdr)
        game_json = get_game_data_from_link(url_prefix + game['link'])

        # Extract Useful Data Sets
        game_id = game['gamePk']
        game_data = game_json.get('gameData')
        live_data = game_json.get('liveData')
        venue_data = game_data.get('venue')
        # Get Game Overview
        game_overview = get_game_overview(
            game_id, game_data, live_data, venue_data)
        
        # Get Team Info
        team_game_info = get_team_info(game_id, game_data, live_data, venue_data)

        # Get Player-Game Stats
        skater_stats_df, goalie_stats_df, scratches_stats_df = get_player_stats(
            live_data, game_data, game_id)

        # Get Play details
        game_plays_info = get_game_plays(game_id, live_data, game_data)

        # Get Play-Player
        game_play_players = get_game_plays_players(game_id, live_data, game_data)

        # Get Shift Data
        game_shift_info = get_shift_data(toi_json)

        ## Insert into DB
        insert_into_db(game_overview, 'games')
        insert_into_db(team_game_info, 'team_game_info')
        insert_into_db(skater_stats_df, 'skater_game_stats')
        insert_into_db(goalie_stats_df, 'goalie_game_stats')
        insert_into_db(scratches_stats_df, 'scratches_game_stats')
        insert_into_db(game_plays_info, 'game_plays_info')
        insert_into_db(game_play_players, 'game_play_players')
        insert_into_db(game_shift_info, 'game_shift_info')
            
if __name__ == '__main__':
    # TODO: potentially use pandas to take single json and break into readable format rather than bits and pieces
    games = pd.read_sql_table('game_schedules', engine)
    
    # games_test = games[games['gamePk'].astype(str).str[:3]=='201'].sample(1)
    games_test = games[games['gamePk'] == 2011010091].sample(1)
    games_test = games.sample(10)
    print(games_test.gamePk)
    get_game_data(games_test)
    print('---------------------')
 
