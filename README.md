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
 * List of Methods
    *
    *
    