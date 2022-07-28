import psycopg2
import psycopg2.extras
import traceback
import datetime
from google.oauth2 import service_account
from google.cloud import exceptions
from google.cloud import bigquery
from google.cloud.bigquery import dbapi
from google.cloud.bigquery.table import Table

# logger = Logger()

class Database:
    def __init__(self, db, *args, **kwargs):
        try:
            if args:
                self.conn = db.connect(*args)
            else:
                self.conn = db.connect(**kwargs)
        except Exception:
            traceback.print_exc()
            return
            # logger.log({'msg': traceback.format_exc()})

    def cursor(self):
        return self.conn.cursor()

    def close_connection(self):
        self.conn.close()

    def execute(self, *args, **kwargs):
        raise NotImplementedError


class Postgres(Database):
    def __init__(self, conn_string, dict_cursor=False):
        super().__init__(psycopg2, conn_string)
        self.conn.autocommit = True
        if dict_cursor:
            self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        else:
            self.cur = self.conn.cursor()
        self.last_query = None

    def execute(self, command, params=None, *args, **kwargs):
        self.last_query = {
            'command': self.cur.mogrify(command, params).decode(),
            'params': params,
        }
        self.cur.execute(command, *args, vars=params, **kwargs)


class BigQuery:
    def __init__(self, conn_string):
        credentials = service_account.Credentials.from_service_account_info(conn_string)
        self.bigquery = bigquery
        self.client = bigquery.Client(credentials=credentials)
        self.conn = dbapi.Connection(self.client)
        self.cur = self.conn.cursor()

    def execute(self, command, params=None, *args, **kwargs):
        self.cur.execute(command, *args, parameters=params, **kwargs)
        return self.cur

    @staticmethod
    def schema_field(
        name,
        field_type,
        mode = 'NULLABLE',
        description = None,
        max_length = None

    ):
        kwargs = {
            'name': name,
            'field_type': field_type,
            'mode': mode,
        }

        if description:
            kwargs['description'] = description
        if max_length:
            kwargs['max_length'] = max_length

        return bigquery.SchemaField(**kwargs)

    @staticmethod
    def table(*args, **kwargs):
        return bigquery.Table(*args, **kwargs)

    def create_table(self, table: Table, if_exists=None):
        try:
            result = self.client.create_table(table)
        except exceptions.Conflict:
            if if_exists == 'REPLACE':
                self.client.delete_table(table)
                result = self.client.create_table(table)
            else:
                raise
        return result

    def get_launch_id(self):
        import uuid
        launch_id = str(uuid.uuid1())
        self.execute(f'''
            INSERT INTO engine.launch_log (launch_id, log_dtime) VALUES ('{launch_id}', '{datetime.datetime.now()}')
        ''')
        print(launch_id)
        return launch_id