# pyspark-utilities
ETL focused utilities library for PySpark

## Setup
1. Install library  
    ```bash
    pip install pyspark-utilities
    ```
2. Follow instructions in [__spark-etl-utilities__](https://github.com/zaksamalik/spark-etl-utilities)
   repo to build `spark-etl-utilities` JAR
3. Load resulting JAR file in Spark session 

## Spark Utilities
### Methods
* `start_spark` - instantiate SparkSession

    ```py
    from os.path import expanduser
    from pyspark import SparkConf
    
    config = (SparkConf().setAll([
        ('spark.driver.extraClassPath', expanduser('/path/to/jars/*')),
        ('spark.executor.extraClassPath', expanduser('/path/to/jars/*'))
    ]))
    spark = start_spark(config=config, app_name='spark_udfs_example', env='local')
    ```
    * arguments:
        * `config` (_SparkConf_) - SparkConf() with set parameters (defaulted).
        * `app_name` (_str_) - name of Spark application (defaulted).
        * `env` (_str_) - where Spark application is running (defaulted). Known values: `local`, `emr`.
        * `enable_hive` (_bool_) - if `True`: adds Hive support via `enableHiveSupport()` (defaulted).
        * `source_aws_credentials_file` (_bool_) - whether to source AWS credentials file (defaulted).
        * `aws_profile` (_str_) - name of profile to use for interacting with AWS services (defaulted).
                                  Only used if `env` is `local`.

## Spark UDFs
### Example
See full example [__here__](https://github.com/zaksamalik/pyspark-utilities/blob/develop/src/spark_udf_testing.py).
```py
from pyspark.sql.functions import col
from pyspark_utilities.spark_udfs import SparkUDFs
# `spark` = instantiated SparkSession
udfs = SparkUDFs(spark)     
# apply UDF
df_with_uuid = (df
                .withColumn('uuid', udfs.generate_uuid())
                .withColumn('clean_string', udfs.clean_string(col('messy_text'))))
``` 
### Methods 
* __General Functions__
    * `clean_string` - remove Java ISO control characters from, and trim, string
        * returns: _string_ (nullable)
    *  `empty_string_to_null` - convert empty strings to null values
        * returns: _string_ (nullable)
    *  `generate_uuid` - generate V4 UUID
        * returns: _string_
    * `map_booleans_ynu` - map boolean values to `Y`, `N`, `Unknown`
        * returns: _string_
    * `string_to_double_pfd` - convert string to double (where `.` represents decimal place)
        * returns: _double_ (nullable)
    * `string_to_double_cfd` - convert string to decimal (where `,` represents decimal place)
        * returns: _double_ (nullable)
    * `string_is_number` - validate whether passed string could be converted to a number.
        * returns: _boolean_
* __Datetime Functions__
    * `normalize_date_md` - normalize string to date with MONTH before DAY
        * returns: _date_ (nullable)
    * `normalize_date_dm` - normalize string to date with DAY before MONTH
        * returns: _date_ (nullable)
    * `normalize_timestamp_md` - normalize string to timestamp with MONTH before DAY
        * returns: _timestamp_ (nullable)
    * `normalize_timestamp_dm` - normalize string to timestamp with DAY before MONTH
        * returns: _timestamp_ (nullable)
