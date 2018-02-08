from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from postgres_plugin.hooks.karmic_postgres_hook import KarmicPostgresHook

from airflow.utils.decorators import apply_defaults
import json
import logging


class PostgresToS3Operator(BaseOperator):
    """
    PostgreSQL to CSV file in /tmp/

    """

    template_fields = ['start', 'end', 's3_key']

    @apply_defaults
    def __init__(self,
                 postgres_conn_id,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 start=None,
                 end=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.start = start
        self.end = end

    def execute(self, context):
        hook = KarmicPostgresHook(self.postgres_conn_id)
        tables = list(hook.get_tables())
        self.copy_records(hook, tables)

    def copy_records(self, hook, tables):
        """
        Use COPY command to copy all tables into CSV files stored in /tmp/
        """
        logging.info('Initiating record retrieval.')
        logging.info('Start Date: {0}'.format(self.start))
        logging.info('End Date: {0}'.format(self.end))

        for tablename in tables:
            filename = '/tmp/{0}.csv'.format(tablename)
            query = \
                """
                COPY {0}
                TO {1}
                DELIMITER ',' CSV HEADER;
                """.format(tablename, filename)
            hook.copy_expert(query, filename)
            self.s3_upload(filename)

    def s3_upload(self, file):
        s3 = S3Hook(s3_conn_id=self.s3_conn_id)
        key = '{0}'.format(self.s3_key)
        s3.load_file(
            filename=file,
            bucket_name=self.s3_bucket,
            key=key,
            replace=True
        )
        s3.connection.close()
        logging.info('File uploaded to s3')
