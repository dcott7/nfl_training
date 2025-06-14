from datetime import datetime
from io import StringIO
import sqlite3

import pandas as pd
import requests

BASE_URL = "https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play"
# YEARS = range(1999, datetime.now().year)
YEARS = range(datetime.now().year-1, datetime.now().year)

def create_table(conn, table, name):
    table.to_sql(name, conn, if_exists='replace', index=False)

def subset_unique(df, cols):
    return df[cols].drop_duplicates()

def fetch_pbp_data(years):
    pbp_dfs = []
    for year in years:
        print(f"Fetching play-by-play data for year: {year}")
        response = requests.get(f"{BASE_URL}_{year}.csv")
        response.raise_for_status()  # Raises error if download fails
        df = pd.read_csv(StringIO(response.text))
        pbp_dfs.append(df)

    return pd.concat(pbp_dfs, ignore_index=True)

def fetch_teams(pbp):
    teams = subset_unique(pbp,['home_team']).reset_index(drop=True)
    teams['team_id'] = teams.index + 1
    teams = teams.rename(columns={'home_team': 'team_name'})
    return teams[['team_id','team_name']]

def create_teams(conn, data):
    create_table(conn, data, "teams")

def fetch_team_id_map(conn):
    return pd.read_sql_query(
        "SELECT team_id, team_name FROM teams", conn
    ).set_index('team_name')['team_id'].to_dict()

def fetch_games(pbp, team_id_map):
    cols = ['game_id','home_team','away_team','season_type','week','game_date','season','div_game']
    games = subset_unique(pbp,cols).copy()
    games['home_team_id'] = games['home_team'].map(team_id_map)
    games['away_team_id'] = games['away_team'].map(team_id_map)
    games = games.drop(columns=['home_team', 'away_team'])
    return games[[
        'game_id','season','season_type','week','game_date','div_game','home_team_id','away_team_id'
    ]]

def create_games(conn, data):
    create_table(conn, data, "games")

def fetch_game_lines(pbp):
    game_line = subset_unique(pbp,['game_id', 'spread_line', 'total_line']).copy()
    game_line['home_spread_line'] = game_line['spread_line']
    return game_line.drop_duplicates()

def create_game_lines(conn, data):
    create_table(conn, data, 'game_lines')

def fetch_stadiums(pbp):
    return subset_unique(pbp,['stadium_id','stadium'])

def create_stadiums(conn, data):
    create_table(conn, data,'stadiums')

def fetch_coaches(pbp):
    coaches = subset_unique(pbp,['home_coach']).reset_index(drop=True)
    coaches['coach_id'] = coaches.index + 1
    coaches = coaches.rename(columns={'home_coach': 'coach_name'})
    return coaches[['coach_id','coach_name']]

def create_coaches(conn, data):
    create_table(conn, data, "coaches")

def fetch_coach_id_map(conn):
    return pd.read_sql_query(
        "SELECT coach_id, coach_name FROM coaches", conn
    ).set_index('coach_name')['coach_id'].to_dict()

def fetch_game_coaches(pbp, coach_id_map):
    game_coaches = subset_unique(pbp,['game_id','home_coach','away_coach']).copy()
    game_coaches['home_coach_id'] = game_coaches['home_coach'].map(coach_id_map)
    game_coaches['away_coach_id'] = game_coaches['away_coach'].map(coach_id_map)
    game_coaches = game_coaches.drop(columns=['home_coach','away_coach'])
    return game_coaches

def create_game_coaches(conn, data):
    create_table(conn, data,'game_coaches')

def fetch_game_details(pbp):
    return subset_unique(pbp,['game_id','start_time','stadium_id','location','roof','surface'])

def create_game_details(conn, data):
    create_table(conn, data, 'game_details')

def fetch_game_weather(pbp):
    return subset_unique(pbp,['game_id','weather','temp','wind'])

def create_game_weather(conn, data):
    create_table(conn, data,'game_weather')

def fetch_game_results(pbp):
    return subset_unique(pbp,['game_id','home_score','away_score'])

def create_game_results(conn, data):
    create_table(conn, data,'game_results')

def fetch_drives(pbp):
    cols = [
        'game_id', 'drive', 'drive_play_count', 'drive_first_downs',
        'drive_inside20', 'drive_ended_with_score', 'drive_yards_penalized',
        'drive_start_transition', 'drive_end_transition',
        'drive_start_yard_line', 'drive_end_yard_line', 'ydsnet'
    ]
    pbp_filtered = pbp[pbp['drive'].notna()]
    drives = subset_unique(pbp_filtered, cols)
    drives['drive_yards'] = drives['ydsnet']
    drives = drives.drop(columns=['ydsnet'])

    return drives

def create_drives(conn, data):
    create_table(conn, data, 'drives')

def fetch_drive_plays(pbp):
    pbp_filtered =  pbp[pbp['paly'==1]]
    return subset_unique(pbp_filtered, ['game_id','drive','play_id'])

def fetch_drive_times(pbp):
    cols = [
        'game_id', 'drive', 'drive_quarter_start', 'drive_quarter_end',
        'drive_game_clock_start', 'drive_game_clock_end'
    ]
    pbp_filtered = pbp[pbp['drive'].notna()]
    return subset_unique(pbp_filtered, cols)

def create_drive_times(conn, data):
    create_table(conn, data, 'drive_times')

def fetch_timeouts(pbp, team_id_map):
    """Fetches the timeouts called on every play"""
    cols = ['game_id','play_id','timeout_team']
    pbp_filtered = pbp[pbp['timeout']==1]
    pbp_filtered['timeout_team_id'] = pbp_filtered['timeout_team'].map(team_id_map)
    return subset_unique(pbp_filtered, cols)

def create_timeouts(conn, data):
    create_table(conn, data, 'timeouts')

def fetch_play_score(pbp, team_id_map):
    """Fetches the score changes."""
    pbp_filtered = pbp[pbp['sp']==1].copy()
    pbp_filtered = subset_unique(pbp_filtered,[
        'game_id','play_id','td_team','touchdown','field_goal_result','safety','posteam','defteam',
        'extra_point_result','two_point_conv_result','defensive_two_point_conv','defensive_extra_point_conv'
    ])

    def determine_score_team(row):
        if row['touchdown'] == 1:
            return row['td_team']
        elif row['safety'] == 1:
            return row['defteam']
        elif row['field_goal_result'] == 'made':
            return row['posteam']
        elif row['extra_point_result'] == 'good':
            return row['posteam']
        elif row['two_point_conv_result'] == 'success':
            return row['posteam']
        elif row.get('defensive_two_point_conv') == 1:
            return row['defteam']
        elif row.get('defensive_extra_point_conv') == 1:
            return row['defteam']
        else:
            return None

    def determine_points(row):
        if row['touchdown'] == 1:
            return 6
        elif row['safety'] == 1:
            return 2
        elif row['field_goal_result'] == 'made':
            return 3
        elif row['extra_point_result'] == 'good':
            return 1
        elif row['two_point_conv_result'] == 'success':
            return 2
        elif row.get('defensive_two_point_conv') == 1:
            return 2
        elif row.get('defensive_extra_point_conv') == 1:
            return 2
        else:
            return 0
        
    pbp_filtered['scoring_team'] = pbp_filtered.apply(determine_score_team, axis=1)
    pbp_filtered['scoring_team_id'] = pbp_filtered['scoring_team'].map(team_id_map)
    pbp_filtered['points'] = pbp_filtered.apply(determine_points, axis=1)
    return pbp_filtered[['game_id', 'play_id', 'scoring_team_id', 'points']]

def create_play_score(conn, data):
    create_table(conn, data, 'play_scores')

def fetch_down_distance(pbp):
    """Fetches the down and distance before every play"""
    pbp_filtered = pbp[(pbp['down'].notna()) & (pbp['play']==1)]
    return subset_unique(pbp_filtered, ['game_id','play_id','down','ydstogo','yardline_100','goal_to_go'])

def create_down_distance(conn, data):
    create_table(conn, data, 'down_distance')

def fetch_play_description(pbp):
    pbp_filtered = pbp[pbp['play']==1]
    return subset_unique(pbp_filtered,['game_id','play_id','desc'])

def create_play_description(conn, data):
    create_table(conn, data, 'play_description')

def fetch_non_play_description(pbp):
    pbp_filtered = pbp[pbp['play']==0]
    return subset_unique(pbp_filtered,['game_id','play_id','desc'])

def create_non_play_description(conn, data):
    create_table(conn, data, 'non_play_description')

def fetch_play_time(pbp):
    """Fetches the game times before each play"""
    cols = ['game_id','play_id','qtr','quarter_seconds_remaining','play_clock']
    return subset_unique(pbp, cols)

def fetch_play_formation(pbp):
    cols = ['game_id','drive','play_id','shotgun']
    return subset_unique(pbp, cols)

def fetch_play_description(pbp):
    return subset_unique(pbp, ['game_id','drive','play_id','desc'])

# def fetch_play_flags(pbp): # TODO: rename... ppl might think flags as penalties
#     cols = [
#         'game_id','drive','play_id','no_huddle','qb_kneel','qb_dropback',
#         'qb_spike','qb_scramble','touchback','replay_or_challenge',
#         'aborted_play'
#     ]
#     return subset_unique(pbp, cols) # TODO: cast posteam to pos_team_id and create a def_team_id

def fetch_play_result(pbp):
    return subset_unique(pbp,['game_id','drive','play_id','sp','yards_gained','touchdown'])

def fetch_pass_play_info(pbp):
    return pbp[[
        'game_id','drive','play_id','pass_length','pass_location','air_yards',
        'yards_after_catch',
    ]]

def fetch_run_play_info(pbp):
    return subset_unique(pbp,['game_id','drive','play_id','run_location','run_gap'])

def fetch_play_probabilities(pbp):
    cols = [
        'game_id','drive','play_id','no_score_prob','opp_fg_prob',
        'opp_safety_prob','opp_td_prob','fg_prob','safety_prob',
        'td_prob','extra_point_prob','two_point_conversion_prob',
        'total_home_epa','total_away_epa','total_home_rush_epa',
        'total_away_rush_epa','total_home_pass_epa','total_away_pass_epa',
        'home_wp','away_wp','vegas_home_wp','total_home_rush_wpa',
        'total_away_rush_wpa','total_home_pass_wpa','total_away_pass_wpa'
    ]
    return subset_unique(pbp, cols)

def fetch_plays():
    return

def main():
    conn = sqlite3.connect('nfl.db')
    pbp = fetch_pbp_data(YEARS)
    pbp.to_csv("pbp.csv")
    
    create_teams(conn, fetch_teams(pbp))
    team_map = fetch_team_id_map(conn)
    create_games(conn, fetch_games(pbp, team_map))
    create_stadiums(conn, fetch_stadiums(pbp))
    create_game_details(conn, fetch_game_details(pbp))
    create_game_weather(conn, fetch_game_weather(pbp))
    create_game_results(conn, fetch_game_results(pbp))
    create_coaches(conn, fetch_coaches(pbp))
    coach_map = fetch_coach_id_map(conn)
    create_game_coaches(conn, fetch_game_coaches(pbp, coach_map))
    create_game_lines(conn, fetch_game_lines(pbp))
    create_drives(conn, fetch_drives(pbp))
    create_drive_times(conn, fetch_drive_times(pbp))
    create_timeouts(conn, fetch_timeouts(pbp, team_map))
    create_play_score(conn, fetch_play_score(pbp, team_map))
    create_down_distance(conn, fetch_down_distance(pbp))
    create_play_description(conn, fetch_play_description(pbp))
    create_non_play_description(conn, fetch_non_play_description(pbp))

    conn.close()

if __name__ == '__main__':
    main()