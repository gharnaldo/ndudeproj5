from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    STAGING_SQL = """
    copy {} from '{}' 
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    region '{}' format as JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 aws_conn_id="",
                 redshift_conn_id = "",
                 table = "",
                 s3_path = "",
                 region= "us-west-2",
                 data_format = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_path = s3_path
        self.region = region
        self.data_format = data_format        

    def execute(self, context):
        self.log.info('Running StageToRedshiftOperator')
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        self.log.info('Hooked to S3')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift_hook.run(StageToRedshiftOperator.STAGING_SQL.format(self.table,self.s3_path,credentials.access_key, credentials.secret_key,self.region,self.data_format))
        





