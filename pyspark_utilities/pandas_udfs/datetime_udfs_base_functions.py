import holidays
import pandas as pd
from .general_udfs_base_functions import clean_string


def is_holiday_usa(dt):
    """ Check whether a given date is a US holiday.

    Args:
        dt (Date, Timestamp): date to check for holiday status.

    Returns:
        str: `Y`, `N`, or `Unknown`
    """
    if dt is None:
        return 'Unknown'
    elif pd.to_datetime(dt) in holidays.US():
        return 'Y'
    else:
        return 'N'


def to_datetime_md(dt_str):
    """ Apply `pd.to_datetime` with inferring datetime and null handling (MONTH comes BEFORE DAY).

    Args:
        dt_str (str): target str to parse to datetime.

    Returns:
        Timestamp: parsed from string.
    """
    if clean_string(dt_str) is None:
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
    if clean_string(dt_str) is None:
        return None
    else:
        return pd.to_datetime(dt_str, infer_datetime_format=True, dayfirst=True)
