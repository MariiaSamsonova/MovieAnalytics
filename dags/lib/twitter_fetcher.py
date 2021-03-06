import json
from datetime import date
import os

from searchtweets import gen_request_parameters, load_credentials, collect_results

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"

def fetch_data_from_twitter(**kwargs):
   print(os.path.expanduser('~'))
   tweets = query_data_from_twitter()
   store_twitter_data(tweets)

def query_data_from_twitter():
   query = gen_request_parameters("#movie OR #cinema", None, tweet_fields = 'text,lang,created_at,geo,public_metrics', results_per_call=100)#500K
   print("We are getting data from Twitter ...", query)
   search_args = load_credentials("~/twitter_keys.yaml", yaml_key="search_tweets_v2", env_overwrite=False)
   return collect_results(query, max_tweets=100, result_stream_args=search_args)#500K


def store_twitter_data(tweets):
   current_day = date.today().strftime("%Y%m%d")
   TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/twitter/Movie/" + current_day + "/"
   if not os.path.exists(TARGET_PATH):
       os.makedirs(TARGET_PATH)
   print("Writing here: ", TARGET_PATH)
   f = open(TARGET_PATH + "twitter.json", "w+")
   f.write(json.dumps(tweets, indent=4))