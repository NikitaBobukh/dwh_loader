### Intro
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

Use yml to create as much users as you need

```python
    command:
      - -c
      - airflow users list || ( airflow db init &&
        airflow users create
          --role Admin
          --username airflow
          --password airflow
          --email airflow@airflow.com
          --firstname airflow
          --lastname airflow )
```

Docker yml includes only web-server, scheduler and pg. 

`+  Dockerfile expand yml with requirements install

To expand docker use
```python
    docker build . --tag extended_airflow:latest --no-cache
```
To run docker use
```python
    docker-compose up -d
```

### Сonfig
Settings / keys and access codes to databases and other services. This folder is not stored in the repository and is distributed manually at the discretion of the owner.


### Loader
Data loaders. One loader is provided for each source (table/view).

The loader must contain the following components:

```python
    class BrandsLoader(PostgresLoader):
        default_source_table = 'public.brands' # source table name
        default_target_table = 'brands' # BQ table name
        default_dbname = 'lavka' # source database name
        default_partitioning_field = 'created_at' # date field for incremental load. If it is missed - each load will upload full table
        default_clustering_field = 'id' # field for clustering
        default_date_trunc = 'day' # incremental load period

    if __name__ == '__main__':
        BrandsLoader().run()
        exit()
```

All loader classes for loading data from postgres should be inherited from PostgresLoader

```python
    from PostgresLoaders import PostgresLoader
```

The name of the loader class must match the name of the file in which it is located.
```python
    class BatchesLoader(PostgresLoader):
```

After loader created, it shoud be added to DAG. DAGs use bash operatior to run
```python
    pg_brands_load = BashOperator(
        task_id='BrandsLoader',
        bash_command="cd /opt/airflow/loader; python3 -m Loaders.BrandsLoader --start_date {{ ds_nodash }}"
    )
```