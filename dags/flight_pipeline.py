from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'flight_etl_pipeline',
    default_args=default_args,
    description='Pipeline complet : migration → ingestion OpenSky → ETL PostgreSQL, toutes les 5 minutes',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['flight'],
) as dag:

    # Applique les migrations SQL (idempotent, skip si déjà appliquées)
    run_migrate = BashOperator(
        task_id='run_migration',
        bash_command='cd /opt/airflow/project && python src/migrate.py',
    )

    # Récupère les états OpenSky → MinIO + enrichissement Geoapify
    run_ingest = BashOperator(
        task_id='run_ingestion',
        bash_command='cd /opt/airflow/project && python src/main.py',
    )

    # ETL : MinIO → transformation geopandas → PostgreSQL
    run_etl = BashOperator(
        task_id='run_etl',
        bash_command='cd /opt/airflow/project && python etl.py',
    )

    run_migrate >> run_ingest >> run_etl
