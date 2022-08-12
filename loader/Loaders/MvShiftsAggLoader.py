from BQLoader import BQLoader

class BrandsAggLoader(BQLoader):
    default_sql_file = 'mv_shifts_agg.sql'

if __name__ == '__main__':
    BrandsAggLoader().run()
    exit()