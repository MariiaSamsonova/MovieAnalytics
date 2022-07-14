import os
from datetime import date

from pyspark.sql import SQLContext
from pyspark import SparkContext

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"
current_day =  date.today().strftime("%Y%m%d")


def imdb(sqlContext):
   FORMATED_PATH = DATALAKE_ROOT_FOLDER + "formatted/imdb/MovieRating/" + current_day + "/"
   USAGE_OUTPUT_FOLDER = DATALAKE_ROOT_FOLDER + "usage/imdb/" + current_day + "/"
   if not os.path.exists(USAGE_OUTPUT_FOLDER):
       os.makedirs(USAGE_OUTPUT_FOLDER)
   separate_df = sqlContext.read.parquet(FORMATED_PATH+"title.ratings.snappy.parquet")
   separate_df.registerTempTable("ratings")
   separate_df = sqlContext.read.parquet(FORMATED_PATH+"title.akas.snappy.parquet")
   separate_df.registerTempTable("akas")
   separate_df = sqlContext.read.parquet(FORMATED_PATH+"title.basics.snappy.parquet")
   separate_df.registerTempTable("basics")

   combined_df = sqlContext.sql("SELECT titleId, ordering, title, region, language, "
                                "types, attributes, averageRating, numVotes, titleType,"
                                "genres, runtimeMinutes, startYear "
                                "FROM akas, ratings, basics WHERE akas.titleId = ratings.tconst "
                                "AND akas.titleId = basics.tconst LIMIT 5000 ")
   combined_df.write.save(USAGE_OUTPUT_FOLDER+ "imdb_data", format= "parquet", mode="overwrite")

def twitter(sqlContext):
   USAGE_OUTPUT_FOLDER = DATALAKE_ROOT_FOLDER + "usage/twitter/" + current_day + "/"
   TWEETS_PATH = DATALAKE_ROOT_FOLDER + "formatted/twitter/Movie/" + current_day + "/"
   df_twitter = sqlContext.read.parquet(TWEETS_PATH)
   df_twitter.registerTempTable("twitter")
   df = df_twitter.selectExpr("inline_outer(data)")
   df.write.save(USAGE_OUTPUT_FOLDER+ "twitter_data", format= "parquet", mode="overwrite")

def combine_data():
   # sc = SparkContext(appName="CombineData")
   # sqlContext = SQLContext(sc)
   # imdb(sqlContext)
   # twitter(sqlContext)
   i = 0
   while i < 1000000:
      i += 1
