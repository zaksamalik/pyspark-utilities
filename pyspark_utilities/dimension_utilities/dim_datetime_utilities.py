import pandas as pd
from pyspark.sql.functions import (col, concat, date_format, datediff, dayofmonth, dayofweek, dayofyear, expr,
                                   last_day, lit, lpad, quarter, regexp_replace, to_date, weekofyear, when)
from ..pandas_udfs.datetime_udfs import pd_is_holiday_usa


def generate_dim_date(spark, start_year=1901, number_years_out_from_start=300):
    """Create `dim_date` table containing various date feature columns.

    Args:
        spark (SparkSession): Instantiated SparkSession
        start_year (int): starting year for dim_date table.
        number_years_out_from_start (int): number out from `start_year` to increment.

    Returns:
        Spark DataFrame.
    """
    years = [start_year + i for i in range(number_years_out_from_start + 1)]
    months = [i for i in range(1, 13)]
    days = [i for i in range(1, 32)]

    years_df = spark.createDataFrame(pd.DataFrame({'year': years, 'temp_join_key': '1'}))
    months_df = spark.createDataFrame(pd.DataFrame({'month': months, 'temp_join_key': '1'}))
    days_df = spark.createDataFrame(pd.DataFrame({'day_of_month': days, 'temp_join_key': '1'}))

    years_months_df = (years_df
                       .join(months_df,
                             ['temp_join_key'],
                             how='inner'))

    years_month_days_df = (years_months_df
                           .join(days_df,
                                 ['temp_join_key'],
                                 how='inner'))

    date_keys = (years_month_days_df
                 .withColumn('date', to_date(concat(col('year'),
                                                    lpad(col('month'), 2, '0'),
                                                    lpad(col('day_of_month'), 2, '0')), 'yyyyMMdd'))
                 # remove invalid dates
                 .filter("date IS NOT NULL")
                 .withColumn('date_key', regexp_replace(col('date').cast('string'), '-', '').cast('integer')))

    date_features = (date_keys
                     # get `week` and `quarter`
                     .withColumn('week', weekofyear(col('date')))
                     .withColumn('quarter', quarter(col('date')))
                     # get `day_name` and `month_name`
                     .withColumn('day_name', date_format(col('date'), 'EEEE'))
                     .withColumn('month_name', date_format(col('date'), 'MMMM'))
                     # get `date_year`, `date_quarter`, `date_month`, `date_week`
                     .withColumn('date_week', expr("MIN(date) OVER(PARTITION BY week, year)"))
                     .withColumn('date_month', date_format(col('date'), 'yyyy-MM-01'))
                     .withColumn('date_quarter', expr("MIN(date) OVER(PARTITION BY quarter, year)"))
                     .withColumn('date_year', date_format(col('date'), 'yyyy-01-01'))
                     # get `day_of_week`, `day_of_quarter`, `day_of_year`
                     .withColumn('day_of_week', dayofweek(col('date')))
                     .withColumn('day_of_quarter', datediff(col('date'), col('date_quarter')) + lit(1))
                     .withColumn('day_of_year', dayofyear(col('date')))
                     # get `weekend_flag`, `us_holiday_flag`, `business_day_flag`, `leap_year_flag`,
                     # `month_start_flag`, `month_end_flag`
                     .withColumn('weekend_flag', when(col('day_of_week').isin([7, 1]), 'Y').otherwise('N'))
                     .withColumn('us_holiday_flag', pd_is_holiday_usa(col('date').cast('timestamp')))
                     .withColumn('us_biz_day_flag', when((col('weekend_flag') == lit('Y')) |
                                                         (col('us_holiday_flag') == lit('Y')), 'Y').otherwise('N'))
                     .withColumn('leap_year_flag',
                                 when(dayofmonth(last_day(concat(col('year'), lit('-02-01')).cast('date'))) == 29, 'Y')
                                 .otherwise('N'))
                     .withColumn('month_start_flag', when(col('day_of_month') == lit(1), 'Y').otherwise('N'))
                     .withColumn('month_end_flag', when(col('date') == last_day(col('date')), 'Y').otherwise('N'))
                     # get `pct_into_month`, `pct_into_quarter`, `pct_into_year`
                     .withColumn('pct_into_month',
                                 (col('day_of_month') / dayofmonth(last_day(col('date')))).cast('decimal(7, 6)'))
                     .withColumn('date_quarter_end',
                                 when(col('quarter') == lit(1), concat(col('year'), lit('-03-31')))
                                 .when(col('quarter') == lit(2), concat(col('year'), lit('-06-30')))
                                 .when(col('quarter') == lit(3), concat(col('year'), lit('-09-30')))
                                 .when(col('quarter') == lit(4), concat(col('year'), lit('-12-31')))
                                 .otherwise(None)
                                 .cast('date'))
                     .withColumn('days_in_quarter', datediff(col('date_quarter_end'), col('date_quarter')) + lit(1))
                     .withColumn('pct_into_quarter',
                                 (col('day_of_quarter') / col('days_in_quarter')).cast('decimal(7, 6)'))
                     .withColumn('pct_into_year',
                                 (col('day_of_year') / when(col('leap_year_flag') == lit('Y'), 366.0).otherwise(365.0))
                                 .cast('decimal(7, 6)'))
                     # get seasons
                     .withColumn('season_northern',
                                 when(col('month').isin(12, 1, 2), 'Winter')
                                 .when(col('month').isin(3, 4, 5), 'Spring')
                                 .when(col('month').isin(6, 7, 8), 'Summer')
                                 .when(col('month').isin(9, 10, 11), 'Fall')
                                 .otherwise('UNKNOWN'))
                     .withColumn('season_southern',
                                 when(col('month').isin(6, 7, 8), 'Winter')
                                 .when(col('month').isin(9, 10, 11), 'Spring')
                                 .when(col('month').isin(12, 1, 2), 'Summer')
                                 .when(col('month').isin(3, 4, 5), 'Fall')
                                 .otherwise('UNKNOWN')))

    dim_date = (date_features
                .sort('date')
                .select(['date_key',
                         'date',
                         'date_week',
                         'date_month',
                         'date_quarter',
                         'date_year',
                         'day_of_week',
                         'day_of_month',
                         'day_of_quarter',
                         'day_of_year',
                         'week',
                         'month',
                         'quarter',
                         'year',
                         'days_in_quarter',
                         'day_name',
                         'month_name',
                         'season_northern',
                         'season_southern',
                         'weekend_flag',
                         'us_holiday_flag',
                         'us_biz_day_flag',
                         'month_start_flag',
                         'month_end_flag',
                         'leap_year_flag',
                         'pct_into_month',
                         'pct_into_quarter',
                         'pct_into_year']))
    return dim_date


# TODO: `generate_dim_time` (seconds)
