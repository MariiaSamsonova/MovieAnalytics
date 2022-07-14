import os
from datetime import date

import pandas as pd

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"

file_name = "twitter.json"
current_day = date.today().strftime("%Y%m%d")

def convert_twitter_raw_to_formatted():
   RATING_PATH = DATALAKE_ROOT_FOLDER + "raw/twitter/Movie/" + current_day + "/" + file_name
   FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/twitter/Movie/" + current_day + "/"
   if not os.path.exists(FORMATTED_RATING_FOLDER):
       os.makedirs(FORMATTED_RATING_FOLDER)
   df = pd.read_json(RATING_PATH)
   parquet_file_name = file_name.replace(".json", ".snappy.parquet")
   final_df = pd.DataFrame(data=df.data)
   final_df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)
