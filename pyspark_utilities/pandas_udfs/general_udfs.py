import pandas as pd
import re
from uuid import uuid4
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StringType


# noinspection PyArgumentList
@pandas_udf(StringType(), PandasUDFType.SCALAR)
def pd_clean_string(target_col):
    """ Apply `clean_string` over Spark Column.

    Args:
        target_col (Spark Column): containing strings to clean.

    Returns:
        Spark Column (StringType): cleaned version of input strings.
    """
    return pd.Series(target_col.apply(lambda x: clean_string(x)))


def clean_string(target_str):
    """ Remove ISO control characters and trim input string,

    Args:
        target_str (st): string to be cleaned.

    Returns:
        str: cleaned input string.
    """
    if target_str is None:
        return None
    else:
        return re.sub(r'[\x00-\x1F]+', '', target_str).strip()


# noinspection PyArgumentList
@pandas_udf(StringType(), PandasUDFType.SCALAR)
def pd_empty_string_to_null(target_col):
    """ Apply `empty_string_to_null` to Spark Column.

    Args:
        target_col (Spark Column): containing strings to convert empties --> nulls

    Returns:
        Spark Column: where empty strings replaced with nulls.
    """
    return pd.Series(target_col.apply(lambda x: empty_string_to_null(x)))


def empty_string_to_null(target_str):
    """ Check if input string is empty, and return null if so (otherwise return input string).

    Args:
        target_str (str): string to check for emptiness.

    Returns:
        str: null if input string is empty else input string.
    """
    if target_str is None:
        return None
    elif target_str.strip() == '':
        return None
    else:
        return target_str


def pd_generate_uuid(target_col):
    """ Generate UUID v4.

    Args:
        target_col (Spark Column): any column, not actually used... Pandas UDFs require input column.

    Returns:
        Spark Column (StringType): UUID v4.
    """
    return pd.Series(target_col.apply(lambda x: uuid4().__str__()))


def pd_map_booleans_ynu(target_col):
    """ Apply `map_booleans_ynu` over Spark Column.

    Args:
        target_col (Spark Column): containing values to check if they represent booleans / indicators.

    Returns:
        Spark Column (StringType): `Y`, `N`, `Unknown`.
    """
    return pd.Series(target_col.apply(lambda x: map_booleans_ynu(x)))


def map_booleans_ynu(target_val):
    """ Map boolean values to `Y`, `N`, `Unknown`.

    Args:
        target_val (any): value to check if it represents a boolean / indicator.

    Returns:
        str: `Y`, `N`, `Unknown`
    """
    if target_val in [False, 0, '0', 'f', 'F', 'false', 'False', 'FALSE', 'n', 'N', 'no', 'No', 'NO']:
        return 'N'
    elif target_val in [True, 1, '1', 't', 'T', 'true', 'True', 'TRUE', 'y', 'Y', 'yes', 'Yes' 'YES']:
        return 'Y'
    else:
        return 'Unknown'


# def pd_string_to_double_pfd
# def pd_string_to_double_cfd
# def pd_string_is_number
