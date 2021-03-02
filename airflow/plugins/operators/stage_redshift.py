from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """ Stage data from S3 to Redshift

        Arguments:
            redshift_conn_id {str}: redshift connection id
            aws_credentials_id {str}: AWS credentials id
            table {str}: Name of staging table
            s3_bucket {str}: Name of S3 bucket
            s3_key {str}: S3 key
            sql_create {str}: SQL code to create staging table
            sql_stage {str}: SQL code to insert data to staging table
            json_path {str}: Filepath to json file with paths to files in S3
        Returns:
            N/A
    """
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                redshift_conn_id='',
                aws_credentials_id='',
                table='',
                s3_bucket='',
                s3_key='',
                sql_create='',
                sql_stage='',
                json_path='auto',
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id =  redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table= table
        self.sql_create = sql_create
        self.sql_stage = sql_stage
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id, client_type='s3')
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Dropping table from Redshift")
        redshift.run("DROP TABLE IF EXISTS {}".format(self.table))

        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        sql_create_format = self.sql_create.format(self.table)
        self.log.info(f"Creating Table: {self.table} in Redshift")
        redshift.run(sql_create_format)

        sql_stage_format = self.sql_stage.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )


        self.log.info(f"Copying data from S3 to Redshift's Table: {self.table}")
        redshift.run(sql_stage_format)
        self.log.info("Copying data from S3 to Redshift succesfully")




