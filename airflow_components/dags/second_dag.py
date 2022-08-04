from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='our_first_dag_v5',
    description='This is our first dag that we write',
    start_date=datetime(2021, 7, 29, 2),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="cd /opt/airflow; python3 -m Loaders.BrandsLoader"
        # bash_command="python3 -m Loaders.BrandsLoader"
    )