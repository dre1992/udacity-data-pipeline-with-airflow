from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
     Loads the staging data to the analytics dimension table provided

         :param redshift_conn_id: the connection id to redshift
         :param table: the table the data will be inserted
         :param sql_stmt: the sql statement to import the data
     """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_stmt="",
                 truncate=False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.truncate=truncate

    def execute(self, context):
        self.log.info(f'LoadDimensionOperator for {self.table}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.table}")
        formatted_sql = f"INSERT INTO {self.table} ({self.sql_stmt})"
        redshift.run(formatted_sql)
        self.log.info(f"Finished: {self.task_id}")
