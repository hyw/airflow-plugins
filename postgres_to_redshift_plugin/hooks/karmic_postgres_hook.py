from airflow.hooks.postgres_hook import PostgresHook


class KarmicPostgresHook(PostgresHook):
    def get_schema(self, table):
        query = \
            """
            SELECT COLUMN_NAME, COLUMN_TYPE
            FROM COLUMNS
            WHERE TABLE_NAME = '{0}';
            """.format(table)
        self.schema = 'information_schema'
        return super().get_records(query)

    def get_tables(self):
        # get names of all tables
        query = \
            """
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public';
            """
        return super().get_records(query)
