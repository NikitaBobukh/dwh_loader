import datetime
from config.cred.enviroment import Environment
import database


def get_launch_id(creds):
    bq = database.BigQuery(creds)
    return bq.get_launch_id()

def explode_object_name(object_name):
    table_parts = object_name.split('.')
    if len(table_parts) == 2:
        schema, table = table_parts
    else:
        raise ValueError(f'Incorrect source table format: {object_name}')

    return schema, table
