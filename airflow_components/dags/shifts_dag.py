from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='ShiftsDAG',
    description='Extract shifts data from courier',
    start_date=datetime(2022, 8, 8),
    schedule_interval='0 1 * * *',
    catchup=True
) as dag:

    pg_shifts_load = BashOperator(
        task_id='ShiftsLoader',
        bash_command="cd /opt/airflow/loader; python3 -m Loaders.ShiftsLoader --start_date {{ ds_nodash }}"
    )

    pg_shifts_reasons_load = BashOperator(
        task_id='ShiftReasonsLoader',
        bash_command="cd /opt/airflow/loader; python3 -m Loaders.ShiftReasonsLoader --start_date {{ ds_nodash }}"
    )