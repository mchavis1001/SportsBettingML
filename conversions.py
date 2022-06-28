import pandas as pd

short_nba_teams = {
    'UTA': 'Utah Jazz',
    'PHX': 'Phoenix Suns',
    'PHI': 'Philadelphia 76ers',
    'BKN': 'Brooklyn Nets',
    'DEN': 'Denver Nuggets',
    'LAC': 'LA Clippers',
    'MIL': 'Milwaukee Bucks',
    'DAL': 'Dallas Mavericks',
    'LAL': 'Los Angeles Lakers',
    'POR': 'Portland Trail Blazers',
    'ATL': 'Atlanta Hawks',
    'NYK': 'New York Knicks',
    'MIA': 'Miami Heat',
    'GSW': 'Golden State Warriors',
    'MEM': 'Memphis Grizzlies',
    'BOS': 'Boston Celtics',
    'IND': 'Indiana Pacers',
    'WAS': 'Washington Wizards',
    'CHA': 'Charlotte Hornets',
    'SAS': 'San Antonio Spurs',
    'CHI': 'Chicago Bulls',
    'NOP': 'New Orleans Pelicans',
    'SAC': 'Sacramento Kings',
    'TOR': 'Toronto Raptors',
    'MIN': 'Minnesota Timberwolves',
    'CLE': 'Cleveland Cavaliers',
    'OKC': 'Oklahoma City Thunder',
    'ORL': 'Orlando Magic',
    'DET': 'Detroit Pistons',
    'HOU': 'Houston Rockets'
}

nba_abv_teams = pd.DataFrame([short_nba_teams]).T.reset_index()
nba_abv_teams.columns = ['ABV', 'TEAM']

