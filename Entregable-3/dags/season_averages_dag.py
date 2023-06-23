from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}


with DAG(
    dag_id="season_averages_dag",
    default_args= default_args,
    description="DAG Entregable3",
    start_date=datetime.utcnow(),
    schedule_interval='@hourly' ) as dag:
    
    task1= DockerOperator(
    task_id='docker_task',
    image='entregable-3-python',
    api_version='auto',
    auto_remove=True,
    environment={'BASE_API_URL' : 'https://www.balldontlie.io', 'SEASON_AVERAGES_PATH' : '/api/v1/season_averages', 'REDSHIFT_HOST' : 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com', 'REDSHIFT_SCHEMA_NAME' : 'santi_prates7_coderhouse', 'DB_HOST' : 'localhost', 'DB_PORT' : '5439', 'DB_DATA_BASE' : 'data-engineer-database', 'DB_TABLE_NAME' : 'average_player_season_stats', 'DB_USER' : 'santi_prates7_coderhouse'},
    dag=dag
    )
task1