from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Copies the s3 files to redshift using the COPY command

        :param redshift_conn_id: the connection id to redshift
        :param aws_credentials_id: the aws credentials connection id
        :param table: the table the data will be copied to
        :param s3_bucket: the source bucket
        :param s3_key: the prefix key
        :param file_format: the format of the fields (CSV,JSON)
        :param json_options: copy json options
        :param region: the aws region
    """

    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
          COPY {table}
          FROM '{s3_path}'
          ACCESS_KEY_ID '{key}'
          SECRET_ACCESS_KEY '{secret}'
          FORMAT  {format} 
          REGION '{region}'
          """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_format="JSON",
                 json_options="auto",
                 region="us-west-2",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.file_format = file_format
        self.json_options = json_options
        self.region = region

    def execute(self, context):
        self.log.info(f'Starting StageToRedshiftOperator for {self.table}')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info("Retrieved aws credentials")

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        if self.file_format == "CSV":
            formatted_copy_sql = self.copy_sql.format(table=self.table, s3_path=s3_path, key=credentials.access_key,
                                                      secret=credentials.secret_key,
                                                      format="CSV IGNOREHEADER 1 DELIMITER "
                                                             "','", region=self.region)
        else:
            if self.json_options != "auto":
                formatted_copy_sql = self.copy_sql.format(table=self.table, s3_path=s3_path, key=credentials.access_key,
                                                          secret=credentials.secret_key, format=" json 'auto'",
                                                          region=self.region)
            else:
                formatted_copy_sql = self.copy_sql.format(table=self.table, s3_path=s3_path, key=credentials.access_key,
                                                          secret=credentials.secret_key,
                                                          format=" json '{}'".format(self.json_options),
                                                          region=self.region)

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Staging data from s3 to {self.table}")
        redshift.run(formatted_copy_sql)
        self.log.info(f"Finished loading from s3 to {self.table}")
