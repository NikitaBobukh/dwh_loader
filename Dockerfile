FROM apache/airflow:2.3.3
COPY requirements.txt /opt/airflow
USER root
RUN chmod 777 /opt/airflow/requirements.txt
USER airflow
RUN pip install --user -r /opt/airflow/requirements.txt