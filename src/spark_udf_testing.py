from os.path import expanduser
import pandas as pd
from pyspark import SparkConf
from pyspark.sql.functions import col

from pyspark_utilities.spark_utilities import start_spark
from pyspark_utilities.spark_udfs import SparkUDFs


def generate_test_df(spark):
    """ Generate test Spark DataFrame with fields for use with SparkUDFs.

    Args:
        spark (SparkSession): instantiated SparkSession

    Returns:
        Spark DataFrame
    """
    string_values_1 = ['\u0000', None, '', 'abc ', 'abc 123\u0000']
    string_values_2 = [' ', '', None, 'abc ', 'abc 123\u0000']
    boolean_values = ['F', 'FALSE', '0', 'TRUE', 'foo']
    string_double_values_pfd = ['-1', '$100.00', '(100)', '10%', 'bar']
    string_double_values_cfd = ['1', '1.000,000', '4 294 967 295,000', '4 294 967.295,000', '4.294.967.295,000']

    pandas_df = pd.DataFrame({'string_values_1': string_values_1,
                              'string_values_2': string_values_2,
                              'boolean_values': boolean_values,
                              'double_values_pfd': string_double_values_pfd,
                              'double_values_cfd': string_double_values_cfd})

    spark_df = spark.createDataFrame(pandas_df)

    return spark_df


def main():
    # start SparkSession
    config = (SparkConf().setAll([
        ('spark.serializer', 'org.apache.spark.serializer.KryoSerializer'),
        ('spark.driver.extraClassPath', expanduser('~/dep_jar_testing/*')),
        ('spark.executor.extraClassPath', expanduser('~/dep_jar_testing/*'))
    ]))
    spark = start_spark(config=config, app_name='spark_udfs_example', env='local')

    # instantiate Spark UDFs
    udfs = SparkUDFs(spark)

    df_test = generate_test_df(spark)

    df_clean = (df_test
                # generate UUID
                .withColumn('uuid', udfs.generate_uuid())
                # clean string
                .withColumn('clean_string_column', udfs.clean_string(col('string_values_1')))
                # empty string
                .withColumn('empty_string_column', udfs.empty_string_to_null(col('string_values_2')))
                # boolean values
                .withColumn('mapped_booleans_column', udfs.map_booleans_ynu(col('boolean_values')))
                # number to double (period for decimal)
                .withColumn('number_check_pfd_column', udfs.string_is_number(col('double_values_pfd')))
                .withColumn('doubles_pfd_column', udfs.string_to_double_pfd(col('double_values_pfd')))
                # number to double (comma for decimal)
                .withColumn('number_check_cfd_column', udfs.string_is_number(col('double_values_cfd')))
                .withColumn('doubles_cfd_column', udfs.string_to_double_cfd(col('double_values_cfd')))
                .select(['uuid',
                         'clean_string_column',
                         'empty_string_column',
                         'mapped_booleans_column',
                         'number_check_pfd_column',
                         'number_check_cfd_column',
                         'doubles_pfd_column',
                         'doubles_cfd_column']))

    df_clean.show()


if __name__ == '__main__':
    main()
