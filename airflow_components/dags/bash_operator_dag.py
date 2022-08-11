from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='Brand_Loader_Bash',
    description='Extract and load brand data -> Agg in BQ',
    start_date=datetime(2022, 8, 5),
    schedule_interval='5 1 * * *',
    catchup=True
) as dag:

    pg_brand_load = BashOperator(
        task_id='pg_brand_load',
        bash_command="cd /opt/airflow; python3 -m Loaders.BrandsLoader --start_date {{ ds_nodash }}"
    )

    bq_brand_agg = BashOperator(
        task_id='bq_brand_agg',
        bash_command="cd /opt/airflow; python3 -m Loaders.BrandsAggLoader"
    )

    pg_brand_load.set_downstream(bq_brand_agg)