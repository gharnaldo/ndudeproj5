import datetime
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

def load_staging_events_to_redshift(*args, **kwargs):
    """
    Loads staging_events data from S3 to Redshift
    """
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(sql_statements.COPY_STAGING_EVENTS_SQL.format(credentials.access_key, credentials.secret_key))

def load_staging_songs_to_redshift(*args, **kwargs):
    """
    Loads staging_songs data from S3 to Redshift
    """
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(sql_statements.COPY_STAGING_SONGS_SQL.format(credentials.access_key, credentials.secret_key))

def check_greater_than_zero(*args, **kwargs):
    """
    Checks none of the dime tables are empty
    """
    tables = kwargs["params"]
    redshift_hook = PostgresHook("redshift")
    for i in tables:
        logging.info(f"Table {tables[i]}")
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {tables[i]}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {table} contained 0 rows")
        logging.info(f"Data quality on table {tables[i]} check passed with {records[0][0]} records")
    
      
dag = DAG('udac_example_dag2',
          description='Load and transform data in Redshift with Airflow',
          start_date=datetime.datetime.now()
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = PythonOperator(
    task_id='load_staging_events_to_redshift',
    dag=dag,
    python_callable=load_staging_events_to_redshift
)

stage_songs_to_redshift = PythonOperator(
    task_id='load_staging_songs_to_redshift',
    dag=dag,
    python_callable=load_staging_songs_to_redshift
)

load_songplays_table = PostgresOperator(
    task_id='load_songplays_table',
    postgres_conn_id='redshift',
    sql=sql_statements.SONGPLAY_TABLE_LOAD,
    dag=dag
)

load_user_dimension_table = PostgresOperator(
    task_id='load_user_dimension_table',
    postgres_conn_id='redshift',
    sql=sql_statements.USER_TABLE_LOAD,
    dag=dag
)

load_song_dimension_table = PostgresOperator(
    task_id='load_song_dimension_table',
    postgres_conn_id='redshift',
    sql=sql_statements.SONG_TABLE_LOAD,
    dag=dag
)

load_artist_dimension_table = PostgresOperator(
    task_id='load_artist_dimension_table',
    postgres_conn_id='redshift',
    sql=sql_statements.ARTIST_TABLE_LOAD,
    dag=dag
)

load_time_dimension_table = PostgresOperator(
    task_id='load_time_dimension_table',
    postgres_conn_id='redshift',
    sql=sql_statements.TIME_TABLE_LOAD,
    dag=dag
)


run_quality_checks = PythonOperator(
    task_id='check_stations_data',
    dag=dag,
    python_callable=check_greater_than_zero,
    provide_context=True,
    params={'table_songs': 'songs', 'table_artists': 'artists','table_users': 'users', 'table_times': 'times'}
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

