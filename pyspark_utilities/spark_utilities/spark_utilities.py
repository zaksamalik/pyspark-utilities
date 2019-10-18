import configparser
from os.path import expanduser
from warnings import warn

from pyspark import SparkConf
from pyspark.sql import SparkSession


def start_spark(config=SparkConf(),
                app_name=None,
                env='cluster',
                enable_hive=False,
                source_aws_credentials_file=False,
                aws_profile=None):
    """Instantiate SparkSession.

    Args:
        config (SparkConf): SparkConf() with set parameters (optional).
        app_name (str): Name of Spark application (optional).
        env (str): Where Spark application is running (required). Known values: `local`, `cluster`.
        enable_hive (bool): If `True`: adds Hive support via `enableHiveSupport()`
        source_aws_credentials_file (bool): Whether to source AWS credentials file.
        aws_profile (str): Name of profile to use for interacting with AWS services. Only used if `env` is `local`.

    Returns:
        Instantiated SparkSession.
    """
    # validate inputs
    ok_envs = ['local', 'cluster']
    ok_envs_str = ', '.join(['`' + e + '`' for e in ok_envs])
    assert env in ok_envs, f'Invalid value passed to `env` argument: `{env}`. Acceptable values: {ok_envs_str}.'

    # start SparkSession builder
    if env == 'local':
        if app_name is None:
            app_name = 'some_app'
        session_builder = (SparkSession
                           .builder
                           .master('local')
                           .config(conf=config)
                           .appName(app_name))
    else:
        session_builder = SparkSession.builder.config(conf=config)

    # enable Hive support
    if enable_hive:
        session_builder = session_builder.enableHiveSupport()

    # instantiate SparkSession
    spark = session_builder.getOrCreate()

    # get credentials for AWS profile when running Spark locally
    if source_aws_credentials_file:
        if aws_profile is None:
            warn("`aws_profile` is None with `source_aws_credentials_file` set to true. Using `default` AWS profile.")
            aws_profile = 'default'
        cfp = configparser.ConfigParser()
        cfp.read(expanduser("~/.aws/credentials"))
        access_id = cfp.get(aws_profile, "aws_access_key_id")
        access_key = cfp.get(aws_profile, "aws_secret_access_key")
        # noinspection PyProtectedMember, PyUnresolvedReferences
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.access.key", access_id)
        hadoop_conf.set("fs.s3a.secret.key", access_key)

    return spark
