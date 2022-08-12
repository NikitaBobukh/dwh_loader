from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='mvAggDAG',
    description='Aggregate data to materializations',
    start_date=datetime(2022, 8, 8),
    schedule_interval='0 1 * * *',
    catchup=True
) as dag:

    trigger_dependent_dag = TriggerDagRunOperator(
        task_id="trigger_dependent_dag",
        trigger_dag_id="ShiftsDAG",
        wait_for_completion=True
    )

    pg_brands_load = BashOperator(
        task_id='BrandsLoader',
        bash_command="cd /opt/airflow/loader; python3 -m Loaders.MvShiftsAggLoader"
    )

    trigger_dependent_dag >> pg_brands_load