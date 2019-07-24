import pandas as pd
import re
from uuid import uuid4
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import BooleanType, DoubleType, StringType


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
    """ Remove ISO control characters and trim input string. Returns None if cleaned string is empty.

    Args:
        target_str (st): string to be cleaned.

    Returns:
        str: cleaned input string.
    """
    if target_str is None:
        return None
    else:
        string_clean = re.sub(r'[\x00-\x1F]+', '', target_str).strip()
        if string_clean == '':
            return None
        else:
            return string_clean


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


# noinspection PyArgumentList
@pandas_udf(StringType(), PandasUDFType.SCALAR)
def pd_generate_uuid(target_col):
    """ Generate UUID v4.

    Args:
        target_col (Spark Column): any column, not actually used... Pandas UDFs require input column.

    Returns:
        Spark Column (StringType): UUID v4.
    """
    return pd.Series(target_col.apply(lambda x: uuid4().__str__()))


# noinspection PyArgumentList
@pandas_udf(StringType(), PandasUDFType.SCALAR)
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


# noinspection PyArgumentList
@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def pd_string_to_double_pfd(target_col):
    """ Apply `string_to_double` to Spark Column (PERIOD for DECIMAL place).

    Args:
        target_col (Spark Column): containing double values as strings.

    Returns:
        Spark Column (DoubleType): doubles converted from strings.
    """
    return pd.Series(target_col.apply(lambda x: string_to_float(x, comma_for_decimal=False)))


# noinspection PyArgumentList
@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def pd_string_to_double_cfd(target_col):
    """ Apply `string_to_double` Spark Column (COMMAS for DECIMAL place).

    Args:
        target_col (Spark Column): containing double values as strings.

    Returns:
        Spark Column (DoubleType): doubles converted from strings.
    """
    return pd.Series(target_col.apply(lambda x: string_to_float(x, comma_for_decimal=True)))


# noinspection PyArgumentList
@pandas_udf(BooleanType(), PandasUDFType.SCALAR)
def pd_string_is_number(target_col):
    """ Apply `string_is_number` to Spark Column.

    Args:
        target_col (Spark Column): containing possible numeric values as strings.

    Returns:
        Spark Column (BooleanType): indicator of whether string can be converted to number
    """
    return pd.Series(target_col.apply(lambda x: string_is_number(x)))


def string_to_float(target_str, comma_for_decimal=False):
    """ Convert string to float.

    Args:
        target_str (str): target str to convert to double.
        comma_for_decimal (bool): whether commas represent decimal in passed string.

    Returns:
        float: converted from input string.
    """
    if not string_is_number(target_str):
        return None
    else:
        if comma_for_decimal:
            string_clean = re.sub(',', '.', re.sub('[^0-9,-]', '', target_str.strip()))
        else:
            string_clean = re.sub('[^0-9.-]', '', target_str.strip())
        number_match = extract_number_from_string(string_clean)
        if re.match('\\(.*\\)', target_str):
            return number_match * -1.0
        else:
            return number_match


def extract_number_from_string(target_str):
    """Extract number from string.

    Args:
        target_str (str): containing number in string format.

    Returns:
        float: parsed from string.
    """
    number_pattern = '(\\-?[0-9]+(\\.[0-9]+)?)'
    matches = re.search(number_pattern, target_str)
    if matches:
        return float(matches.group(0))
    else:
        raise ValueError(f"ERROR: Bad number passing. Could not parse {target_str}.")


def string_is_number(target_str):
    """ Check whether passed string can accurately be converted to a number.

    Args:
        target_str (str): string to validate if parsable to number.

    Returns:
        bool
    """
    if target_str is None:
        return False
    else:
        return bool(re.fullmatch('^\\d+$', re.sub('[^0-9]', '', target_str)))
