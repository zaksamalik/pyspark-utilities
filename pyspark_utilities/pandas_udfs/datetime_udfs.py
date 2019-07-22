import holidays
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DateType, StringType, TimestampType


# noinspection PyArgumentList
@pandas_udf(StringType(), PandasUDFType.SCALAR)
def pd_is_holiday_usa(target_col):
    """ Apply `is_holiday_usa` to Spark column.

    Args:
        target_col (Spark Column): containing dates or timestamps to check holiday status.

    Returns:
        Spark Column (StringType): `Y`, `N`, or `Unknown`.
    """
    return target_col.apply(lambda x: is_holiday_usa(x))


def is_holiday_usa(dt):
    """ Check whether a given date is a US holiday.

    Args:
        dt (str, Timestamp):

    Returns:
        str: `Y`, `N`, or `Unknown`
    """
    if dt is None:
        return 'Unknown'
    elif pd.to_datetime(dt) in holidays.US():
        return 'Y'
    else:
        return 'N'


# noinspection PyArgumentList
@pandas_udf(DateType(), PandasUDFType.SCALAR)
def pd_normalize_date_md(target_col):
    """ Convert column with dates as strings to dates (MONTH BEFORE DAY).

    Args:
        target_col (Spark Column): containing dates as strings.

    Returns:
        Spark Column (DateType): containing dates extracted from strings.
    """
    pd.Series(target_col.apply(lambda x: to_datetime_md(x)))


# noinspection PyArgumentList
@pandas_udf(DateType(), PandasUDFType.SCALAR)
def pd_normalize_date_dm(target_col):
    """ Convert column with dates as strings to dates (DAY BEFORE MONTH).

    Args:
        target_col (Spark Column): containing dates as strings.

    Returns:
        Spark Column (DateType): containing dates extracted from strings.
    """
    pd.Series(target_col.apply(lambda x: to_datetime_dm(x)))


# noinspection PyArgumentList
@pandas_udf(TimestampType(), PandasUDFType.SCALAR)
def pd_normalize_timestamp_md(target_col):
    """ Convert column with timestamps as strings to timestamps (MONTH BEFORE DAY).

    Args:
        target_col (Spark Column): containing dates as strings.

    Returns:
        Spark Column (TimestampType): containing dates extracted from strings.
    """
    pd.Series(target_col.apply(lambda x: to_datetime_md(x)))


# noinspection PyArgumentList
@pandas_udf(TimestampType(), PandasUDFType.SCALAR)
def pd_normalize_timestamp_dm(target_col):
    """ Convert column with timestamps as strings to timestamps (DAY BEFORE MONTH).

    Args:
        target_col (Spark Column): containing dates as strings.

    Returns:
        Spark Column (TimestampType): containing dates extracted from strings.
    """
    pd.Series(target_col.apply(lambda x: to_datetime_dm(x)))


def to_datetime_md(dt_str):
    """ Apply `pd.to_datetime` with inferring datetime and null handling (MONTH comes BEFORE DAY).

    Args:
        dt_str (str): target str to parse to datetime.

    Returns:
        Timestamp: parsed from string.
    """
    if dt_str is None or dt_str.strip() == '':
        return None
    else:
        return pd.to_datetime(dt_str, infer_datetime_format=True, dayfirst=False)


def to_datetime_dm(dt_str):
    """ Apply `pd.to_datetime` with inferring datetime and null handling (DAY comes BEFORE MONTH).

    Args:
        dt_str (str): target str to parse to datetime.

    Returns:
        Timestamp: parsed from string.
    """
    if dt_str is None:
        return None
    else:
        return pd.to_datetime(dt_str, infer_datetime_format=True, dayfirst=True)
