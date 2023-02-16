from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

import sql_statements

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
    'retries': 2
}


dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date=datetime.now()
        )
    
    
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='load_staging_events_to_redshift',
    provide_context=False,
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id = "redshift",
    table = "staging_events",
    s3_path = "s3://udacity-dend/log_data",
    region= "us-west-2",
    data_format = "s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='load_staging_songs_to_redshift',
    provide_context=False,
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id = "redshift",
    table = "staging_songs",
    s3_path = "s3://udacity-dend/song_data/A/A/A/TRAAAAK128F9318786.json",
    region= "us-west-2",
    data_format = "auto"
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    redshift_conn_id = "redshift",
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dimension_table',
    redshift_conn_id='redshift',
    sql="user_table_insert",
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dimension_table',
    redshift_conn_id='redshift',
    sql="song_table_insert",
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dimension_table',
    redshift_conn_id='redshift',
    sql="artist_table_insert",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dimension_table',
    redshift_conn_id='redshift',
    sql="time_table_insert",
    dag=dag
)


run_quality_checks = DataQualityOperator(
    task_id='check_stations_data',
    dag=dag,
    provide_context=True,
    params={'table_songs': 'songs', 'table_artists': 'artists','table_users': 'users', 'table_times': 'times'},
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift


stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

