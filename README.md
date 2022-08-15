### Loader
The repository is a set of utilities for downloading and processing data from one or many sources in Google Big Query

### Docker
Basic airflow docker volumes:
```python
    - ./airflow_components/dags:/opt/airflow/dags
    - ./airflow_components/logs:/opt/airflow/logs
    - ./airflow_components/plugins:/opt/airflow/plugins
```
Volume containce main logic
```python
    - ./loader:/opt/airflow/loader
```
Docker yml includes only web-server, scheduler and pg. 
`+  Dockerfile expand yml with requirements install