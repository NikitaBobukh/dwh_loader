from PostgresLoaders import PostgresLoader


class BrandsLoader(PostgresLoader):
    default_source_table = 'public.brands'
    default_target_table = 'brands'
    default_dbname = 'lavka'
    default_partitioning_field = 'created_at'
    default_clustering_field = 'id'
    default_date_trunc = 'day'

if __name__ == '__main__':
    BrandsLoader().run()
    exit()
