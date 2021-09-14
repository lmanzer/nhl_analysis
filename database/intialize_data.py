import pandas as pd
import urllib.request
import json
import sqlite3
import sqlalchemy as sa

from .settings import DATABASE_NAME


# Settings
engine = sa.create_engine(f'sqlite:///{DATABASE_NAME}')

# Get all season data
url_seasons = 'https://statsapi.web.nhl.com/api/v1/seasons'

with urllib.request.urlopen(url_seasons) as url:
    seasons_json = json.loads(url.read().decode())
    
seasons_df = pd.DataFrame(seasons_json.get('seasons'))
seasons_df

seasons_df.to_sql('seasons',engine)