from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """ Load data to fact table

        Arguments:
            table {str}:  Name of table where data to be inserted
            redshift_conn_id {str}: redshift connection id
            sql_create {str}: SQL code to create table
            sql_select {str}: SQL code to select the data to be inserted
        Returns:
            N/A
    """

    ui_color = '#F98866'
    sql_insert = """
        INSERT INTO {} ({});
    """

    @apply_defaults
    def __init__(self,
                table = '',
                redshift_conn_id = '',
                sql_create = '',
                sql_select = '',
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_create = sql_create
        self.sql_select = sql_select

    def execute(self, context):
        self.log.info(f'Building Fact Table:{self.table}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run("DROP TABLE IF EXISTS {}".format(self.table))
        redshift.run(self.sql_create)
        sql_stage_format = LoadFactOperator.sql_insert.format(self.table, self.sql_select)
        redshift.run(sql_stage_format)
