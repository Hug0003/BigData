from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'flight_etl_pipeline',
    default_args=default_args,
    description='Run extraction (main.py) and transform/load (etl.py) every 5 minutes',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['flight'],
) as dag:

    # Task to ingest raw data into MinIO
    run_main = BashOperator(
        task_id='run_ingestion',
        bash_command='cd /opt/airflow/project && python src/main.py',
    )

    # Task to transform and load into PostgreSQL
    run_etl = BashOperator(
        task_id='run_etl',
        bash_command='cd /opt/airflow/project && python etl.py',
    )

    run_main >> run_etl
