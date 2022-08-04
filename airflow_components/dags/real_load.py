from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="example_loader",
    schedule_interval='* 10 * * * *',
    start_date=datetime(year=2022, month=8, day=1),
    catchup=False
) as dag:

    def brand_loader():
        import os
        print(os.getcwd())
        # from PostgresLoaders import PostgresLoader

        # class BrandsLoader(PostgresLoader):
        #     default_source_table = 'public.brands'
        #     default_target_table = 'brands'
        #     default_dbname = 'lavka'
        #     default_partitioning_field = 'created_at'
        #     default_clustering_field = 'id'
        #     default_date_trunc = 'day'
        
        # BrandsLoader().run()

    run_this = PythonOperator(
        task_id="brand_loader",
        python_callable=brand_loader
    )
