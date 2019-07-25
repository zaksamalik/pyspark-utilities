import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DateType, StringType, TimestampType
from .datetime_udfs_base_functions import is_holiday_usa, to_datetime_md, to_datetime_dm


# noinspection PyArgumentList
@pandas_udf(StringType(), PandasUDFType.SCALAR)
def pd_is_holiday_usa(target_col):
    """ Apply `is_holiday_usa` to Spark column.

    Args:
        target_col (Spark Column): containing dates or timestamps to check holiday status.

    Returns:
        Spark Column (StringType): `Y`, `N`, or `Unknown`.
    """
    return pd.Series(target_col.apply(lambda x: is_holiday_usa(x)))


# noinspection PyArgumentList
@pandas_udf(DateType(), PandasUDFType.SCALAR)
def pd_normalize_date_md(target_col):
    """ Convert column with dates as strings to dates (MONTH BEFORE DAY).

    Args:
        target_col (Spark Column): containing dates as strings.

    Returns:
        Spark Column (DateType): containing dates extracted from strings.
    """
    return pd.Series(target_col.apply(lambda x: to_datetime_md(x)))


# noinspection PyArgumentList
@pandas_udf(DateType(), PandasUDFType.SCALAR)
def pd_normalize_date_dm(target_col):
    """ Convert column with dates as strings to dates (DAY BEFORE MONTH).

    Args:
        target_col (Spark Column): containing dates as strings.

    Returns:
        Spark Column (DateType): containing dates extracted from strings.
    """
    return pd.Series(target_col.apply(lambda x: to_datetime_dm(x)))


# noinspection PyArgumentList
@pandas_udf(TimestampType(), PandasUDFType.SCALAR)
def pd_normalize_timestamp_md(target_col):
    """ Convert column with timestamps as strings to timestamps (MONTH BEFORE DAY).

    Args:
        target_col (Spark Column): containing dates as strings.

    Returns:
        Spark Column (TimestampType): containing dates extracted from strings.
    """
    return pd.Series(target_col.apply(lambda x: to_datetime_md(x)))


# noinspection PyArgumentList
@pandas_udf(TimestampType(), PandasUDFType.SCALAR)
def pd_normalize_timestamp_dm(target_col):
    """ Convert column with timestamps as strings to timestamps (DAY BEFORE MONTH).

    Args:
        target_col (Spark Column): containing dates as strings.

    Returns:
        Spark Column (TimestampType): containing dates extracted from strings.
    """
    return pd.Series(target_col.apply(lambda x: to_datetime_dm(x)))
