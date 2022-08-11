from BQLoader import BQLoader

class BrandsAggLoader(BQLoader):
    default_sql_file = 'brand_test.sql'

if __name__ == '__main__':
    BrandsAggLoader().run()
    exit()