from airflow.plugins_manager import AirflowPlugin
from postgres_to_redshift_plugin.hooks.karmic_postgres_hook import KarmicPostgresHook
from postgres_to_redshift_plugin.operators.postgres_to_s3_operator import PostgresToS3Operator
from postgres_to_redshift_plugin.operators.s3_to_redshift_operator import S3ToRedshiftOperator


class PostgresToRedshiftPlugin(AirflowPlugin):
    name = "PostgresToRedshiftPlugin"
    hooks = [KarmicPostgresHook]
    operators = [PostgresToS3Operator, S3ToRedshiftOperator]
    # Leave in for explicitness
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
