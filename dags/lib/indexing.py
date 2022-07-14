import os
from pyspark.sql import SQLContext, conf
from elasticsearch import Elasticsearch, helpers
from datetime import  date
from pyspark import SparkContext
from pyspark.sql.functions import lit

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"
current_day = date.today().strftime("%Y%m%d")

es = Elasticsearch(hosts=["http://localhost:9200"])
#docs = []

def indexing():
    sc = SparkContext(appName="CombineData")
    sqlContext = SQLContext(sc)
                                                                                            #filename
    ##IMDB

    IMDB_USAGE_PATH = DATALAKE_ROOT_FOLDER + "usage/imdb/" +  current_day + "/imdb_data/"
    df = sqlContext.read.parquet(IMDB_USAGE_PATH)
    index = 0
    es.indices.delete(index="imdb", ignore=[400, 404])
    es.indices.create(index="imdb", ignore=400)
    es.indices.put_mapping(
        index="imdb",
        ignore=400,
        body={
                "properties": {
                    "titleId": {
                        "type": "string"
                    },
                    "title": {
                        "type": "string",
                    },
                    "region": {
                        "type": "string"
                    },
                    "language": {
                        "type": "string",
                    },
                    "types": {
                        "type": "string",
                    },
                    "attributes": {
                        "type": "string",
                    },
                    "averageRating": {
                        "type": "string"
                    },
                    "numVotes": {
                        "type": "string"

                    },
                    "titleType": {
                        "type": "string"

                    },
                    "genres": {
                        "type": "string"

                    },
                    "runtimeMinutes": {
                        "type": "long"

                    },
                    "startYear": {
                        "type": "long"

                    }

                }
        }
    )

    arr = {}
    cols = df.dtypes
    for row in df.take(300000):
        try:
            i = 0

            for param in row:
                if cols[i][0] == "runtimeMinutes":
                    if param == "\\N":
                        param = "0"
                arr[cols[i][0]] = param
                i+=1

            es.index(index="imdb", body=arr)
            print(index)

        except Exception as e:
            print('error: ' + str(e) + ' in' + str(index))
        index += 1

    ##TWITTER
    TWITTER_USAGE_PATH = DATALAKE_ROOT_FOLDER + "usage/twitter/" +  current_day + "/twitter_data/"
    df = sqlContext.read.parquet(TWITTER_USAGE_PATH)
    index = 0
    es.indices.delete(index="twitter", ignore=[400, 404])
    es.indices.create(index="twitter", ignore=400)
    es.indices.put_mapping(
        index="twitter",
        ignore=400,
        body={
            "properties": {
                "created_at": {
                    "type": "date"
                },
                "id": {
                    "type": "string",
                },
                "lang": {
                    "type": "string",
                },
                "public_metrics": {
                    "type": "object",
                },
                "text": {
                    "type": "string"
                }
            }
        }
    )

    arr = {}
    cols = df.dtypes
    for row in df.take(10):
        try:
            i = 0

            for param in row:
                # print(cols[i][0] + " " + param)
                arr[cols[i][0]] = param
                i += 1

            es.index(index="twitter", body=arr)
            print(index)

        except Exception as e:
            print('error: ' + str(e) + ' in' + str(index))
        index += 1
    i = 0
    while i < 1000000:
        i+=1


