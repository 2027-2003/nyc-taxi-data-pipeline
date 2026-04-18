from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "data_engineering_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    ingestion = BashOperator(
        task_id="ingestion",
        bash_command="python ingestion/data_collector.py"
    )

    cleaning = BashOperator(
        task_id="cleaning",
        bash_command="python cleaning/clean_data.py"
    )

    transform = BashOperator(
        task_id="spark_transform",
        bash_command="python spark_jobs/transform_data.py"
    )

    quality = BashOperator(
        task_id="data_quality",
        bash_command="python quality_checks/data_validation.py"
    )

    ingestion >> cleaning >> transform >> quality