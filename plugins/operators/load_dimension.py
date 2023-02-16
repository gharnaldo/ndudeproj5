from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql = "", 
                 append_only = False,
                 table = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.append_only = append_only

    def execute(self, context):
        self.log.info(f"LoadDimensionOperator {self.sql}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append_only:
            self.log.info("Table {} truncated!".format(self.table))
            redshift.run("TRUNCATE TABLE {}".format(self.table))          
        redshift.run(getattr(SqlQueries,self.sql))        
