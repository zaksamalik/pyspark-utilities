import pandas as pd
from uuid import uuid4
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType, StringType
from .general_udfs_base_functions import (clean_string, empty_string_to_null, map_booleans_ynu,
                                          string_to_double_pfd, string_to_double_cfd)


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


# noinspection PyArgumentList
@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def pd_string_to_double_pfd(target_col):
    """ Apply `string_to_double` to Spark Column (PERIOD for DECIMAL place).

    Args:
        target_col (Spark Column): containing double values as strings.

    Returns:
        Spark Column (DoubleType): doubles converted from strings.
    """
    return pd.Series(target_col.apply(lambda x: string_to_double_pfd(x, comma_for_decimal=False)))


# noinspection PyArgumentList
@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def pd_string_to_double_cfd(target_col):
    """ Apply `string_to_double` Spark Column (COMMAS for DECIMAL place).

    Args:
        target_col (Spark Column): containing double values as strings.

    Returns:
        Spark Column (DoubleType): doubles converted from strings.
    """
    return pd.Series(target_col.apply(lambda x: string_to_double_cfd(x, comma_for_decimal=True)))
