from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.twitter_fetcher import fetch_data_from_twitter
from lib.imdb_fetcher import fetch_data_from_imdb
from lib.raw_to_fmt_twitter import convert_twitter_raw_to_formatted
from lib.raw_to_fmt_imdb import convert_imdb_raw_to_formatted
from lib.indexing import indexing
from lib.combine_data import combine_data

with DAG(
       'movie_analytics_dag',
       default_args={
           'depends_on_past': False,
           'email': ['airflow@example.com'],
           'email_on_failure': False,
           'email_on_retry': False,
           'retries': 1,
           'retry_delay': timedelta(seconds=15),
       },
       description='DAG',
       schedule_interval=None,
       start_date=datetime(2021, 1, 1),
       catchup=False,
       tags=['example'],
) as dag:
   dag.doc_md = """
       This is my DAG in airflow that automates the work of the project.
   """

   task1_twitter = PythonOperator(
       task_id='fetch_data_from_twitter',
       python_callable=fetch_data_from_twitter,
       provide_context=True,
       op_kwargs={'task_number': 'task1_twitter'}
   )

   task1_imdb = PythonOperator(
       task_id='fetch_data_from_imdb',
       python_callable=fetch_data_from_imdb,
       provide_context=True,
       op_kwargs={'task_number': 'task1_imdb'}
   )

   task2_twitter = PythonOperator(
       task_id='convert_twitter_raw_to_formatted',
       python_callable=convert_twitter_raw_to_formatted,
       provide_context=True,
       op_kwargs={'task_number': 'task2_twitter'}
   )

   task2_imdb = PythonOperator(
       task_id='convert_imdb_raw_to_formatted',
       python_callable=convert_imdb_raw_to_formatted,
   )

   task3_combine = PythonOperator(
       task_id='combine_data',
       python_callable=combine_data,
       provide_context=True,
       op_kwargs={'task_number': 'task3_combine'}
   )

   task4_indexing = PythonOperator(
       task_id='indexing',
       python_callable=indexing,
       provide_context=True,
       op_kwargs={'task_number': 'task4_indexing'}
   )

   task1_twitter >> task2_twitter >> task3_combine >> task4_indexing
   task1_imdb >> task2_imdb >> task3_combine >> task4_indexing













