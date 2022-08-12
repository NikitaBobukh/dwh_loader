from PostgresLoaders import PostgresLoader


class ShiftsLoader(PostgresLoader):
    default_source_table = 'public.shifts'
    default_target_table = 'shifts'
    default_dbname = 'courier'
    default_partitioning_field = 'created_at'
    default_clustering_field = 'id'
    default_date_trunc = 'day'

if __name__ == '__main__':
    ShiftsLoader().run()
    exit()
