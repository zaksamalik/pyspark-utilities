# pyspark-utilities
ETL focused utilities library for PySpark

1. Install library  
    ```bash
    pip install pyspark-utilities
    ```
2. Follow instructions in [spark-etl-utilities](https://github.com/zaksamalik/spark-etl-utilities)
   repo to build `spark-etl-utilities` JAR
3. Load resulting JAR file in Spark session 
    ```py
    config = (SparkConf().setAll([
        ('spark.driver.extraClassPath', expanduser('/path/to/jars/*')),
        ('spark.executor.extraClassPath', expanduser('/path/to/jars/*'))
    ]))
    ```
