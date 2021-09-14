import pandas
import urllib.request
import json

# Note: a lot of this is leveraged from the kaggle NHL data repository (https://www.kaggle.com/martinellis/where-the-data-comes-from)
# and re-written in python

# Settings
sample_game_id = "2019020951"

url_gameData_prefix = "https://statsapi.web.nhl.com/api/v1/game/"
url_toiData_prefix = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId="
url_gameData_suffix = "/feed/live"

game_url =  (url_gameData_prefix + sample_game_id + url_gameData_suffix)


with urllib.request.urlopen(game_url) as url:
    game_data_json = json.loads(url.read().decode())

print(game_data_json)
