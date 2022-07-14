import os
from datetime import date

import pandas as pd
#import pyarrow

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"

akas_file_name = "title.akas.tsv.gz"
ratings_file_name = "title.ratings.tsv.gz"
basics_file_name = "title.basics.tsv.gz"
current_day = date.today().strftime("%Y%m%d")

def convert (file_name):
   RAW_FILEPATH = DATALAKE_ROOT_FOLDER + "raw/imdb/MovieRating/" + current_day + "/" + file_name
   FOLDER_PATH = DATALAKE_ROOT_FOLDER + "formatted/imdb/MovieRating/" + current_day + "/"
   if not os.path.exists(FOLDER_PATH):
       os.makedirs(FOLDER_PATH)
   df = pd.read_csv(RAW_FILEPATH, sep='\t', low_memory=False)
   parquet_file_name = file_name.replace(".tsv.gz", ".snappy.parquet")
   df.to_parquet(FOLDER_PATH + parquet_file_name)

def convert_imdb_raw_to_formatted():
   convert(akas_file_name)
   convert(ratings_file_name)
   convert(basics_file_name)