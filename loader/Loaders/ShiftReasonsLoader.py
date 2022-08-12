from PostgresLoaders import PostgresLoader


class ShiftReasonsLoader(PostgresLoader):
    default_source_table = 'public.shift_reasons'
    default_target_table = 'shift_reasons'
    default_dbname = 'courier'
    default_partitioning_field = 'created_at'
    default_clustering_field = 'id'
    default_date_trunc = 'day'


if __name__ == '__main__':
    ShiftReasonsLoader().run()
    exit()
