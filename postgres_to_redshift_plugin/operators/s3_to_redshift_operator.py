from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook


class S3ToRedshiftOperator(BaseOperator):
    """
    S3 -> Redshift via COPY Commands
    """

    template_fields = ['s3_key']

    base_copy = """
    COPY {redshift_database}.{redshift_table}
    FROM 's3://{s3_bucket}/{s3_key}'
    CREDENTIALS '{conn_str}'
    JSON 'auto';
    """

    def __init__(self,
                 s3_bucket,
                 s3_key,
                 redshift_database,
                 redshift_table,
                 s3_conn_id,
                 redshift_conn_id,
                 *args, **kwargs):

        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.copy = self.base_copy
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_database = redshift_database
        self.redshift_table = redshift_table
        self.s3_conn_id = s3_conn_id
        self.redshift_conn_id = redshift_conn_id

    def build_copy(self):
        """
        Builds copy command returns string representation
        """
        a_key, s_key = S3Hook(s3_conn_id=self.s3_conn_id).get_credentials()
        conn_str = 'aws_access_key_id={};aws_secret_access_key={}'.format(a_key, s_key)

        fmt_str = {
            'redshift_database': self.redshift_database,
            'redshift_table': self.redshift_table,
            's3_bucket': self.s3_bucket,
            's3_key': self.s3_key,
            'conn_str': conn_str
        }

        return self.copy.format(**fmt_str)

    def execute(self, context):
        """
        Runs copy command on redshift
        """
        pg = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql = self.build_copy()
        pg.run(sql)