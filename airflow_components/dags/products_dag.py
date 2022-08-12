from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='ProductsDAG',
    description='Extract products data from courier',
    start_date=datetime(2022, 8, 8),
    schedule_interval='0 1 * * *',
    catchup=True
) as dag:

    pg_brands_load = BashOperator(
        task_id='BrandsLoader',
        bash_command="cd /opt/airflow/loader; python3 -m Loaders.BrandsLoader --start_date {{ ds_nodash }}"
    )