from datetime import datetime, timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'aseficha',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 * * *',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
load_operator = DummyOperator(task_id='Begin_data_loading', dag=dag)

TABLES = ["staging_events", "staging_songs", "users", "songs",
          "artists", "time", "songplays"]

filename = os.path.join(Path(__file__).parents[1], 'create_tables.sql')

f = open(filename)
sqlFile = f.read()
f.close()

# all SQL commands (split on ';')
sqlCommands = sqlFile.split(';')
sql_dict = {}

# Execute every command from the input file
for command in sqlCommands[:-1]:
    sql_dict[command.split(".")[1].split(" (")[0]] = command


def create_dynamic_drop_table(target_table, project_dag):
    """
    Create a postgres operator for dropping the table defined in the arguments

    :parameter target_table: the table to drop
    :parameter project_dag: the dag this operator will be attached
    :return the postgres operator
    """

    task = PostgresOperator(
        task_id=f"drop_{target_table}",
        postgres_conn_id="redshift",
        provide_context=True,
        sql=f"DROP table IF EXISTS {target_table}",
        dag=project_dag,
    )
    return task


def create_dynamic_create_table(target_table, project_dag, sqlstmt):
    """
    Create a postgres operator for creating the table defined in the arguments

    :param target_table: the table to create
    :param project_dag:  the dag this operator will be attached
    :param sqlstmt: the create statement
    :return: the postgres operator
    """
    task = PostgresOperator(
        task_id=f"create_{target_table}",
        postgres_conn_id="redshift",
        provide_context=True,
        sql=sqlstmt,
        dag=project_dag,
    )
    return task


for table in TABLES:
    drop_task = create_dynamic_drop_table(table, dag)
    create_task = create_dynamic_create_table(table, dag, sql_dict[table])
    start_operator >> drop_task
    drop_task >> create_task
    create_task >> load_operator

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    file_format="json",
    json_options="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    file_format="JSON"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_stmt=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql_stmt=SqlQueries.user_table_insert

)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql_stmt=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql_stmt=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql_stmt=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    stmt='select count(*) from songplays where start_time is null;',
    expected_result=0,
)

load_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

stage_songs_to_redshift >> [load_song_dimension_table, load_artist_dimension_table]
stage_events_to_redshift >> [load_user_dimension_table, load_time_dimension_table]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

[load_song_dimension_table,
 load_artist_dimension_table,
 load_songplays_table,
 load_user_dimension_table,
 load_time_dimension_table] >> run_quality_checks

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

run_quality_checks >> end_operator
