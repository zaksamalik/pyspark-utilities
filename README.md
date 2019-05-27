# pyspark-utilities
ETL focused utilities library for PySpark

### Setup
1. Install library  
    ```bash
    pip install pyspark-utilities
    ```
2. Follow instructions in [__spark-etl-utilities__](https://github.com/zaksamalik/spark-etl-utilities)
   repo to build `spark-etl-utilities` JAR
3. Load resulting JAR file in Spark session 
    ```py
    config = (SparkConf().setAll([
        ('spark.driver.extraClassPath', expanduser('/path/to/jars/*')),
        ('spark.executor.extraClassPath', expanduser('/path/to/jars/*'))
    ]))
    ```

### Use SparkUDFs
* Example usage (see full example [__here__](https://github.com/zaksamalik/pyspark-utilities/blob/develop/src/spark_udf_testing.py))
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
### List of Methods 
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




    