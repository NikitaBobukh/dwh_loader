from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="example_python_operator",
    schedule_interval='* 10 * * * *',
    start_date=datetime(year=2022, month=8, day=1),
    catchup=False
) as dag:

    def print_array():
        """Print Numpy array."""
        import numpy as np  # <- THIS IS HOW NUMPY SHOULD BE IMPORTED IN THIS CASE

        a = np.arange(15).reshape(3, 5)
        print(a)
        return a

    run_this = PythonOperator(
        task_id="print_the_context",
        python_callable=print_array,
    )

    # task_fet_datetime = BashOperator