from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='nyc_taxi_pipeline',
    default_args=default_args,
    description='NYC Taxi Data Engineering Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc_taxi', 'data_engineering']
) as dag:

    ingestion = BashOperator(
        task_id='ingestion',
        bash_command='python /app/ingestion/data_collector.py'
    )

    cleaning = BashOperator(
        task_id='cleaning',
        bash_command='python /app/cleaning/clean_data.py'
    )

    transform = BashOperator(
        task_id='transform',
        bash_command='python /app/spark_jobs/transform_data.py'
    )

    quality = BashOperator(
        task_id='quality_checks',
        bash_command='python /app/quality_checks/data_validation.py'
    )

    warehouse = BashOperator(
        task_id='load_warehouse',
        bash_command='python /app/warehouse/load_to_warehouse.py'
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run --project-dir /app/dbt/my_dbt_project'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test --project-dir /app/dbt/my_dbt_project'
    )

    # ترتيب التنفيذ
    ingestion >> cleaning >> transform >> quality >> warehouse >> dbt_run >> dbt_test