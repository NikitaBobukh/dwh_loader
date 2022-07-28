import math
import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from pyparsing import col

import database
from BaseLoader import BaseLoader
from Components.Utils import get_launch_id
from config.cred.enviroment import Environment


class TableModel:
    def __init__(self, target_table):
        self.name = target_table.split('.')[1]
        self.dataset = target_table.split('.')[0]

    def __repr__(self):
        return str(f'{self.dataset}.{self.name}')


class PostgresLoader(BaseLoader):
    TYPES_MAP = {
        'character varying': 'string',
        'uuid': 'string',
        'text': 'string',
        'jsonb': 'string',
        'json': 'string',
        'array': 'string',
        'user-defined':'string',

        'timestamp without time zone': 'TIMESTAMP',
        'time without time zone': 'TIME',
        'date': 'date',

        'bigint': 'int64',
        'smallint': 'int64',
        'integer': 'int64',

        'boolean': 'bool',

        'real': 'float64',
        'double precision': 'float64',
        'numeric': 'float64',
    }
    STRING_TYPES = ['character varying', 'jsonb', 'json', 'array', 'text']

    def __init__(self, name=None, **parameters):
        super().__init__(name, **parameters)
        self.worker_type = 'PostgresLoader'
        self.dbname = self.get_arg('dbname')
        self.target_table = self.get_arg('target_table')
        if self.dbname:
            credentials = getattr(Environment, f'pg_creds_{self.dbname}')
        else:
            credentials = getattr(Environment, f'pg_creds_courier')
        self.pg = database.Postgres(credentials)
        self.bq = database.BigQuery(self.bq_creds)
        self.start, self.end = self.get_interval_dates()
        self.date_trunc = self.get_arg('date_trunc')

    def init_args(self):
        super().init_args()
        self.argparser.add_argument('--source_table', type=str, help='Source table name in PostgreSQL')
        self.argparser.add_argument('--target_table', type=str, help='Target table name in Snowflake')
        self.argparser.add_argument('--cluster_by', type=str, help='Resulting table clustering fields')
        self.argparser.add_argument('--dbname', type=str, help='Database name')
        # необходимые агрументы для инкрементальной загрузки
        self.argparser.add_argument('--start_date', type=int, help='Start date')
        self.argparser.add_argument('--end_date', type=int, help='End date')
        self.argparser.add_argument('--interval', type=int, help='Interval to load as a number of days')
        self.argparser.add_argument('--date_trunc', type=str, help='Атомарность для загрузки')

    def get_interval_dates(self, fmt='%Y%m%d'):
        interval = self.get_arg('interval')
        if interval is None:
            interval = 0

        start_date = str(self.get_arg('start_date')) if self.get_arg('start_date') is not None else ''
        end_date = str(self.get_arg('end_date')) if self.get_arg('end_date') is not None else ''

        if start_date and end_date:
            start = datetime.strptime(start_date, fmt).strftime('%Y-%m-%d')
            end = datetime.strptime(end_date, fmt).strftime('%Y-%m-%d')
            return start, end

        if start_date:
            start = datetime.strptime(start_date, fmt)
            end = (start + timedelta(days=interval))
            return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')

        if end_date:
            end = datetime.strptime(end_date, fmt)
            start = (end - timedelta(days=interval))
            return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')

        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=interval)

        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

    @staticmethod
    def truncate_dates(date_range, date_trunc):
        if date_trunc == 'week':
            tr_dates = date_range.to_period('W-SUN').start_time.drop_duplicates()
        elif date_trunc == 'month':
            tr_dates = (date_range + pd.offsets.MonthBegin(-1)).drop_duplicates()
        elif date_trunc == 'year':
            tr_dates = (date_range + pd.offsets.YearBegin(-1)).drop_duplicates()
        else:
            tr_dates = date_range
        return tr_dates.strftime('%Y-%m-%d')


    def get_source_table_structure(self):
        source_table = self.get_arg('source_table')
        table_parts = source_table.split('.')
        if len(table_parts) == 3:
            catalog, schema, table = table_parts
        elif len(table_parts) == 2:
            catalog = self.dbname
            schema, table = table_parts
        else:
            raise ValueError('Incorrect source table format')

        data = {
            'catalog': catalog,
            'schema': schema,
            'table': table,
        }

        self.pg.execute('''
            SELECT
                column_name,
                is_nullable,
                data_type,
                udt_name
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE
                TABLE_NAME = %(table)s AND
                TABLE_SCHEMA = %(schema)s AND
                TABLE_CATALOG = %(catalog)s;
        ''', data)
        columns = self.pg.cur.fetchall()
        result = {}
        columns_for_length = []
        for column in sorted(columns, key=lambda x: x[0]):
            column_name, is_nullable, data_type, udt_name = column
            data_type = data_type.lower()
            result[column_name] = {
                'is_nullable': True if is_nullable == 'YES' else False,
                'data_type': data_type,
                'data_type_name': udt_name,
            }
            if data_type in self.STRING_TYPES:
                columns_for_length.append((column_name, f'max(length({column_name}::varchar))'))

        self.pg.execute(f'''
            SELECT
                {', '.join(map(lambda x: x[1], columns_for_length))}
            FROM {source_table}
        ''')
        lengths = self.pg.cur.fetchone()
        for column_name, length in zip(map(lambda x: x[0], columns_for_length), lengths):
            result[column_name]['max_size'] = length

        return result

    def generate_bq_table_schema(self, columns):
        schema = []
        for column in columns:
            description = columns[column]
            data_type = self.TYPES_MAP[description['data_type']]
            mode = 'NULLABLE'
            max_size = None
            if data_type == 'string' and description.get('max_size') is not None:
                max_size = description['max_size']
                max_size = 2 ** math.ceil(math.log2(max(1, max_size)))
            if not description['is_nullable']:
                mode = 'REQUIRED'

            schema.append(self.bq.schema_field(name=column, field_type=data_type, mode=mode, max_length=max_size))
        return schema

    def generate_copy_query(self, columns, dt=None):
        string_types = ['character varying', 'text']

        def remove_n(value):
            return f'regexp_replace("{value}", E\'[\\n\\r]+\', \' \', \'g\' )'

        update_flg = max(x == 'updated_at' for x in columns.keys())
        if update_flg:
            where_clause = f'''
            WHERE DATE_TRUNC('{self.date_trunc}', {self.get_arg('partitioning_field')}) = '{dt}'
                OR DATE_TRUNC('{self.date_trunc}', updated_at) = '{dt}'
            '''
        else:
            where_clause = f'''
            WHERE DATE_TRUNC('{self.date_trunc}', {self.get_arg('partitioning_field')}) = '{dt}'
            '''

        if dt:
            return f'''COPY(
                SELECT
                {', '.join(map(lambda k: remove_n(k) if columns[k]['data_type'] in string_types else f'"{k}"', columns.keys()))},
                '{self.launch_id}' as launch_id
                FROM {self.get_arg('source_table')}
                {where_clause}
            ) TO STDOUT WITH CSV DELIMITER ';' QUOTE '"';'''
        else:
            return f'''COPY(
                SELECT
                {', '.join(map(lambda k: remove_n(k) if columns[k]['data_type'] in string_types else f'"{k}"', columns.keys()))},
                '{self.launch_id}' as launch_id
                FROM {self.get_arg('source_table')}
            ) TO STDOUT WITH CSV DELIMITER ';' QUOTE '"';'''

    @property
    def file_name(self):
        return f'''{self.get_arg('dbname')}_{self.get_arg('source_table')}_{self.launch_id}.csv'''

    @property
    def file_path(self):
        return os.path.join('/tmp', self.file_name)

    def create_table_saa(self):
        # создадим или подтвердим наличие таблицы в saa
        # если таблица есть но схема отличается от текущей, падаем с ошибкой
        columns = self.get_source_table_structure()

        launch_id_vals = {
            'is_nullable': False,
            'data_type': 'character varying',
            'data_type_name': 'varchar',
            'max_size': 64
        }

        table_schema = self.generate_bq_table_schema({**columns, **{'launch_id': launch_id_vals}})
        table_id = f'{self.env.project_id}.public.{self.target_table}'
        table = self.bq.table(table_id, schema=table_schema)
        table.clustering_fields = self.get_arg('clustering_field')
        table.time_partitioning = self.bq.bigquery.TimePartitioning(
            type_=self.bq.bigquery.TimePartitioningType.DAY,
            field=self.get_arg('partitioning_field')
        )
        table = self.bq.client.create_table(table, exists_ok=True)  # Make an API request.
        return columns, table_schema

    def data_to_saa(self, dt, columns, saa_schema):
        copy_query = self.generate_copy_query(columns, dt)
        print(copy_query)
        filepath = self.file_path
        with open(filepath, 'w') as file:
            self.pg.cur.copy_expert(copy_query, file)

        dataset_ref = self.bq.client.dataset('public')
        table_ref = dataset_ref.table(self.target_table)
        job_config = self.bq.bigquery.LoadJobConfig(
            source_format=self.bq.bigquery.SourceFormat.CSV,
            write_disposition=self.bq.bigquery.WriteDisposition.WRITE_APPEND,
            skip_leading_rows = 0,
            schema=saa_schema,
            field_delimiter=';',
            quote_character='"'
            )
        
        print(f'read from {filepath}')
        with open(filepath, 'rb') as source_file:
            self.bq.client.load_table_from_file(
                        source_file,
                        table_ref,
                        job_config=job_config,
                    ).result()
            print('write is done')
        
        os.remove(filepath)
    
    def create_table_snp(self, table_schema):
        # создадим или подтвердим наличие таблицы в snp
        # TODO: передовать поля которые нужно парсить если они есть
        # https://cloud.google.com/bigquery/docs/nested-repeated
        # тут пример как создавать nested поля
        # останется только написать sql запрос для парсинга поля и на его основе создать поле для схемы

        table_id = f'{self.env.project_id}.snp.{self.target_table}'
        table = self.bq.table(table_id, schema=table_schema)
        table.clustering_fields = self.get_arg('clustering_field')
        table.time_partitioning = self.bq.bigquery.TimePartitioning(
            type_=self.bq.bigquery.TimePartitioningType.DAY,
            field=self.get_arg('partitioning_field')
        )
        table = self.bq.client.create_table(table, exists_ok=True)
        return None
    
    def data_to_snp(self, dt, columns):
        columns_str = ', '.join([f'`{x}`' for x in columns.keys()]) + ', `launch_id`'

        delete_query = f"""
            DELETE FROM snp.{self.target_table}
            WHERE id IN (
                SELECT DISTINCT id
                FROM saa.{self.target_table}
                WHERE launch_id = '{self.launch_id}'
            );
        """

        print(delete_query)
        rcnt = self.bq.execute(delete_query).rowcount

        upsert_query = f"""
            INSERT INTO snp.{self.target_table} ({columns_str})
            WITH _raw AS (
                SELECT {columns_str}
                FROM `organic-reef-315010.saa.{self.target_table}`
                WHERE launch_id = '{self.launch_id}'
            )
            SELECT *
            FROM _raw
        """

        print(upsert_query)
        rcnt = self.bq.execute(upsert_query).rowcount

    def run(self):
        # создадим или проверим наличие нужных таблиц
        columns, saa_schema = self.create_table_saa()
        self.create_table_snp(saa_schema)
        # если передан параметр date_trunc то запускаем инкриментальную загрузку
        if self.date_trunc:
            dates = pd.date_range(self.start, self.end)
            tr_dates = self.truncate_dates(dates, self.date_trunc)
            for dt in tr_dates:
                self.launch_id = get_launch_id(Environment.gsstorage_creds_json)
                self.data_to_saa(dt, columns, saa_schema)
                # self.data_to_snp(dt, columns)
        # если date_trunc пустой то просто грузим всю таблицу
        else:
            copy_query = self.generate_copy_query(columns)
            filepath = self.file_path
            with open(filepath, 'w') as file:
                self.pg.cur.copy_expert(copy_query, file)

            dataset_ref = self.bq.client.dataset('snp')
            table_ref = dataset_ref.table(self.target_table)
            job_config = self.bq.bigquery.LoadJobConfig(
                source_format=self.bq.bigquery.SourceFormat.CSV,
                write_disposition=self.bq.bigquery.WriteDisposition.WRITE_TRUNCATE,
                skip_leading_rows = 0,
                schema=saa_schema,
                field_delimiter=';',
                quote_character='"'
                )
            
            print(f'read from {filepath}')
            with open(filepath, 'rb') as source_file:
                self.bq.client.load_table_from_file(
                            source_file,
                            table_ref,
                            job_config=job_config,
                        ).result()
                print('write is done')
            
            os.remove(filepath)
