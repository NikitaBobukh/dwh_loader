from BaseLoader import BaseLoader
import database
import datetime
class BQLoader(BaseLoader):
    
    def __init__(self, name=None, **parameters):
        super().__init__(name, **parameters)
        self.bq = database.BigQuery(self.bq_creds)
        self.dir = 'SqlParser'
        self.sql_file = self.get_arg('sql_file')

    def run(self):
        _start = datetime.datetime.today()
        print(f'Loader {self.sql_file} started as {_start.strftime("%Y-%m-%d %H:%M:%S")}')
        with open(f'{self.dir}/{self.sql_file}', 'r') as reader:
            query = reader.read()
        
        self.bq.execute(query)

        _end = datetime.datetime.today()
        _long = (_end - _start).seconds

        print(f'Loader {self.sql_file} finished in {_long} seconds')