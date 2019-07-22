# pyspark-utilities
ETL utilities library for PySpark

## Package Contents
* `spark_utilities` - general PySpark utility functions to develop and run Spark applications
* `pandas_udfs` - Spark UDFs written using [__Pandas UDF__](https://docs.databricks.com/spark/latest/spark-sql/udf-python-pandas.html) functionality added in Spark 2.3
* `spark_udfs` - Python class containing Spark UDFs written in Scala and accessed via jar passed to SparkContext
* `dimension_utilities` - functions to generate dimension tables as Spark DataFrames

## Setup
1. Install library  
    ```bash
    pip install git+https://github.com/zaksamalik/pyspark-utilities
    ```
2. Follow instructions in [__spark-etl-utilities__](https://github.com/zaksamalik/spark-etl-utilities)
   repo to build `spark-etl-utilities` JAR
   * __Note__: steps 2 & 3 are optional and only required in order to use the Spark UDFs written in Scala
3. Load resulting JAR file in Spark session (example in Spark Utilities --> Methods section)

## Spark Utilities
General PySpark utilities functions to develop and run Spark applications
### Methods
__General__
* `start_spark` - instantiate SparkSession
    * arguments:
        * `config` (_SparkConf_) - SparkConf() with set parameters (defaulted).
        * `app_name` (_str_) - name of Spark application (defaulted).
        * `env` (_str_) - where Spark application is running (defaulted). Known values: `local`, `emr`.
        * `enable_hive` (_bool_) - if `True`: adds Hive support via `enableHiveSupport()` (defaulted).
        * `source_aws_credentials_file` (_bool_) - whether to source AWS credentials file (defaulted).
        * `aws_profile` (_str_) - name of profile to use for interacting with AWS services (defaulted).
           Only used if `env` is `local`.
    * example usage
    ```py
    from os.path import expanduser
    from pyspark import SparkConf
    from pyspark_utilities.spark_utilities import start_spark

    config = (SparkConf().setAll([
        ('spark.driver.extraClassPath', expanduser('/path/to/jars/*')),
        ('spark.executor.extraClassPath', expanduser('/path/to/jars/*'))
    ]))
    spark = start_spark(config=config, app_name='example_app', env='local')
    ```

## Pandas UDFs
Spark UDFs written in Python using Pandas UDF functionality added in Spark 2.3.  
Due to vectorization and the use of PyArrow to transfer data from Spark to Pandas,
Pandas UDFs are significantly more performant that row-at-a-time UDFs.

__Note__: all Pandas UDFs start with `pd_` prefix.
### Methods
* __General UDFs__
    * `pd_clean_string` -
    * `pd_clean_string` -
    * `pd_empty_string_to_null` -
    * `pd_generate_uuid` -
    * `pd_map_booleans_ynus` -
    * `pd_string_to_double_pfd` -
    * `pd_string_to_double_cfd` -
    * `pd_string_is_number` -
* __Datetime UDFs__
    * `pd_is_holiday_usa` - check whether a given date is a US holiday (from [__holidays__](https://pypi.org/project/holidays/) package)
        * returns: _StringType_ (`Y`, `N`, `Unknown`)
    * `pd_normalize_date_md` - Convert column with dates as strings to dates
    (MONTH BEFORE DAY).
        * returns: _DateType_
    * `pd_normalize_date_dm` - Convert column with dates as strings to dates
    (DAY BEFORE MONTH).
        * returns: _DateType_
    * `pd_normalize_timestamp_md` - Convert column with timestamps as strings to
    timestamps (MONTH BEFORE DAY).
        * returns _TimestampType_
    * `pd_normalize_timestamp_dm` - Convert column with timestamps as strings to timestamps (DAY BEFORE MONTH).
        * returns _TimestampType_
* example usage TODO
  ```py
  ```

* __Fuzzy String Matching UDFs__ (methods from [__fuzzywuzzy__](https://github.com/seatgeek/fuzzywuzzy) package)
    * `pd_fuzz_ratio` - simple ratio (`fuzz.ratio`)
        * returns: _IntegerType_
    * `pd_fuzz_partial_ratio` - partial ratio (`fuzz.partial_ratio`)
        * returns: _IntegerType_
    * `pd_fuzz_token_set_ratio` - token set ratio (`fuzz.token_set_ratio`)
        * returns: _IntegerType_
    * `pd_fuzz_partial_token_set_ratio` - partial token set ratio (`fuzz.partial_token_set_ratio`)
        * returns: _IntegerType_
    * `pd_fuzz_token_sort_ratio` - token sort ratio (`fuzz.token_sort_ratio`)
        * returns: _IntegerType_
    * `pd_fuzz_partial_token_sort_ratio` - partial token sort ratio (`fuzz.partial_token_sort_ratio`)
        * returns: _IntegerType_  

## Spark UDFs (Scala)
Spark UDFs written in Scala exposed to Python.  

__Important Note__: all Scala Spark UDF functionality also exist as Pandas UDFs.  
Because they are Scala native, these Spark UDFs should be more performant
than Pandas UDFs (in most cases), but require an external JAR in order to use.  
For a pure Python ETL implementation, use Pandas UDFs instead.

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
* __General UDFs__
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
* __Datetime UDFs__
    * `normalize_date_md` - normalize string to date with MONTH before DAY
        * returns: _date_ (nullable)
    * `normalize_date_dm` - normalize string to date with DAY before MONTH
        * returns: _date_ (nullable)
    * `normalize_timestamp_md` - normalize string to timestamp with MONTH before DAY
        * returns: _timestamp_ (nullable)
    * `normalize_timestamp_dm` - normalize string to timestamp with DAY before MONTH
        * returns: _timestamp_ (nullable)

## Dimension Utilities
Functions to generate dimension ("dim") tables as Spark DataFrames.
### Methods
__Datetime__
* `generate_dim_date` - generate Spark DataFrame with various date dimensions (think advanced "dim_date" table).
    * arguments:
        * `spark` - instantiated SparkSession
    * example usage
        ```py
        from pyspark_utilities.spark_utilities import start_spark
        from pyspark_utilities.dimension_utilities import generate_dim_date

        spark=start_spark(env='local')
        dim_date_df = generate_dim_date(spark=spark)
        ```
