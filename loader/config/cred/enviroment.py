import pathlib
import json
from google.oauth2 import service_account
from google.cloud import bigquery, storage

class Environment:
    pg_creds_lavka = "dbname='lavka' user='nbobukh' host='slave01.pgdb.ryadom.kz' password='jurae7ooNg5eiseo4iax0Hee'"
    pg_creds_courier = "dbname='courier' user='nbobukh' host='slave01.pgdb.ryadom.kz' password='jurae7ooNg5eiseo4iax0Hee'"

    with open('organic-reef-315010-1d77f366943c.key', 'r') as f: gsstorage_creds_json = json.load(f)
    project_id = 'organic-reef-315010'
    gc_stagingbucket = 'organic-reef-315010-stage'
    gc_tempbucket = 'organic-reef-315010-temp'
    gc_credentials = service_account.Credentials.from_service_account_info(gsstorage_creds_json)
    bq_client = bigquery.Client(credentials=gc_credentials)
    gcs_client = storage.Client(credentials=gc_credentials)
