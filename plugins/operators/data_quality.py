from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.params = kwargs["params"]


    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        tables = self.params
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for i in tables:
            self.log.info(f"Table {tables[i]}")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {tables[i]}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {tables[i]} OK. Records result: {records[0][0]}")