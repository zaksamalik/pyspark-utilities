# pyspark-utilities
ETL-focused utilities library for PySpark

## Package Contents
* `spark_utilities` - general PySpark utilities to develop and run Spark applications
* `pandas_udfs` - Spark UDFs written using [__Pandas UDF__](https://docs.databricks.com/spark/latest/spark-sql/udf-python-pandas.html) feature [added in Spark 2.3](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)
* `spark_udfs` - Python class containing Spark UDFs written in Scala, accessed via JAR passed to SparkContext at initialization
* `dimension_utilities` - functions to generate dimension tables as Spark DataFrames

## Setup
1. Install library  
    ```bash
    pip install git+https://github.com/zaksamalik/pyspark-utilities
    ```
2. Follow instructions in [__spark-etl-utilities__](https://github.com/zaksamalik/spark-etl-utilities)
   repo to build `spark-etl-utilities` JAR
   * __Note__: steps 2 & 3 are optional and __only__ required in order to use the Spark UDFs written in Scala
3. Load resulting JAR file in Spark session (example in Spark Utilities --> Methods section)

## Spark Utilities
_General PySpark utilities to develop and run Spark applications._
### Methods
__General__
* `start_spark` - instantiate SparkSession
    * arguments:
        * `config` (_SparkConf_) - SparkConf() with set parameters (defaulted).
        * `app_name` (_str_) - name of Spark application (default = None).
        * `env` (_str_) - where Spark application is running (default = 'cluster'). Known values: `local`, `cluster`.
        * `enable_hive` (_bool_) - if `True`: adds Hive support via `enableHiveSupport()` (default = False).
        * `source_aws_credentials_file` (_bool_) - whether to source AWS credentials file (default = False).
        * `aws_profile` (_str_) - name of profile to use for interacting with AWS services (default = None).
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
_User-defined functions written using Pandas UDF feature added in Spark 2.3._

A good introduction of Pandas UDFs can be found [here](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html), but in short: Pandas UDFs are vectorized and use Apache Arrow to transfer data from Spark to Pandas and back, delivering much faster performance than one-row-at-a-time Python UDFs, which are notorious
bottlenecks in PySpark application development.

__Note__: all Pandas UDFs in this library start with a `pd_` prefix.

### Methods
* __General UDFs__
    * `pd_clean_string` - remove ISO control characters from, and trim input string column.
        * returns: _StringType_
    * `pd_empty_string_to_null` - check if input values in strings column are empty, and return null if so.
        * returns: _StringType_
    * `pd_generate_uuid` - generate UUID v4.
        * returns: _StringType_
        * Pandas UDFs require at least one input column, so a column must be passed to this function even though no operations will actually be applied to that column.
        * __Important note__: because Spark is lazy evaluated, UUID will change prior to being written / collected / converted to Pandas DataFrame. Do not rely on UUID as a key throughout Spark application.
    * `pd_map_booleans_ynu` - map boolean values to `Y`, `N`, `Unknown`.
        * returns: _StringType_
    * `pd_string_to_double_pfd` - convert string column to double where PERIOD represents DECIMAL place.
        * returns: _DoubleType_
    * `pd_string_to_double_cfd` - convert string column to double where COMMA represents DECIMAL place.
        * returns: _DoubleType_
    * `pd_string_is_number` - check whether values in string column can be converted to numbers
        * returns: _BooleanType_
* __Datetime UDFs__
    * `pd_is_holiday_usa` - check whether values in date column are US holidays (from [__holidays__](https://pypi.org/project/holidays/) package).
        * returns: _StringType_ (`Y`, `N`, `Unknown`)
    * `pd_normalize_date_md` - Convert column with dates as strings to dates
    (MONTH before DAY).
        * returns: _DateType_
    * `pd_normalize_date_dm` - Convert column with dates as strings to dates
    (DAY before MONTH).
        * returns: _DateType_
    * `pd_normalize_timestamp_md` - Convert column with timestamps as strings to
    timestamps (MONTH before DAY).
        * returns _TimestampType_
    * `pd_normalize_timestamp_dm` - Convert column with timestamps as strings to timestamps (DAY before MONTH).
        * returns _TimestampType_
* example usage
  ```py
  from pyspark_utilities.pandas_udfs import pd_clean_string
  from pyspark.sql.functions import col, lit
  df_clean = (df
              .withColumn('clean_text', pd_clean_string(col('messy_text')))
              .withColumn('uuid', pd_generate_uuid(lit(''))))
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

* example usage
  ```py
  from pyspark.sql.functions import col
  from pyspark_utilities.pandas_udfs import pd_clean_string, pd_fuzz_partial_ratio
  
  df_fuzzy_ratio = (df
                      .withColumn('clean_text1', pd_clean_string(col('messy_text')))
                      .withColumn('clean_text2', pd_clean_string(col('messy_text')))
                      .withColumn('partial_fuzzy_ratio', pd_fuzz_partial_ratio(col('clean_text1'), col('clean_text2'))))                      
  ```
## Spark UDFs (Scala)
_Spark UDFs written in Scala exposed to Python._

__Important Note__: all Scala Spark UDF functionality also exist as Pandas UDFs.  
Because they are Scala native, Spark UDFs should be more performant than Pandas UDFs (in most cases), but require an external JAR in order to use. For pure Python ETL implementations, use Pandas UDFs instead. All other functionality in this package should work fine without the Spark UDFs JAR.

### Example
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
_Functions to generate dimension ("dim") tables as Spark DataFrames._
### Methods
__Datetime__
* `generate_dim_date` - generate Spark DataFrame with various date dimensions
    * arguments:
        * `spark` - instantiated SparkSession
        * `start_date` - starting (minimum) year for dim_date table
        * `number_years_out_from_start` - number of years out from starting date to increment
    * example usage
        ```py
        from pyspark_utilities.spark_utilities import start_spark
        from pyspark_utilities.dimension_utilities import generate_dim_date

        spark=start_spark(env='local')
        dim_date_df = generate_dim_date(spark=spark, start_year=1901, number_years_out_from_start=300)
        ```
