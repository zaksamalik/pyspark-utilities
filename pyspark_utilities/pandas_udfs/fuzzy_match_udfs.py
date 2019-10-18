import pandas as pd
from fuzzywuzzy import fuzz
import jellyfish
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType


# noinspection PyArgumentList
@pandas_udf(IntegerType(), PandasUDFType.SCALAR)
def pd_fuzz_ratio(col1, col2):
    """ Calculate "simple" ratio (`fuzz.ratio`) between two text columns.

    Args:
        col1 (Spark Column): 1st text column
        col2 (Spark Column): 2nd text column

    Returns:
        Spark Column (IntegerType): result of `fuzz.ratio` calculation.
    """
    return pd.Series(map(fuzz.ratio, col1.astype(str), col2.astype(str)))


# noinspection PyArgumentList
@pandas_udf(IntegerType(), PandasUDFType.SCALAR)
def pd_fuzz_partial_ratio(col1, col2):
    """ Calculate "partial" ratio (`fuzz.partial_ratio`) between two text columns.

    Args:
        col1 (Spark Column): 1st text column
        col2 (Spark Column): 2nd text column

    Returns:
        Spark Column (IntegerType): result of `fuzz.partial_ratio` calculation.
    """
    return pd.Series(map(fuzz.partial_ratio, col1.astype(str), col2.astype(str)))


# noinspection PyArgumentList
@pandas_udf(IntegerType(), PandasUDFType.SCALAR)
def pd_fuzz_token_set_ratio(col1, col2):
    """ Calculate "token set" ratio (`fuzz.token_set_ratio`) between two text columns.

    Args:
        col1 (Spark Column): 1st text column
        col2 (Spark Column): 2nd text column

    Returns:
        Spark Column (IntegerType): result of `fuzz.token_set_ratio` calculation.
    """
    return pd.Series(map(fuzz.token_set_ratio, col1.astype(str), col2.astype(str)))


# noinspection PyArgumentList
@pandas_udf(IntegerType(), PandasUDFType.SCALAR)
def pd_fuzz_partial_token_set_ratio(col1, col2):
    """ Calculate "partial token set" ratio (`fuzz.partial_token_set_ratio`) between two text columns.

    Args:
        col1 (Spark Column): 1st text column
        col2 (Spark Column): 2nd text column

    Returns:
        Spark Column (IntegerType): result of `fuzz.partial_token_set_ratio` calculation.
    """
    return pd.Series(map(fuzz.partial_token_set_ratio, col1.astype(str), col2.astype(str)))


# noinspection PyArgumentList
@pandas_udf(IntegerType(), PandasUDFType.SCALAR)
def pd_fuzz_token_sort_ratio(col1, col2):
    """ Calculate "token sort" ratio (`fuzz.token_sort_ratio`) between two text columns.

    Args:
        col1 (Spark Column): 1st text column
        col2 (Spark Column): 2nd text column

    Returns:
        Spark Column (IntegerType): result of `fuzz.token_sort_ratio` calculation.
    """
    return pd.Series(map(fuzz.token_sort_ratio, col1.astype(str), col2.astype(str)))


# noinspection PyArgumentList
@pandas_udf(IntegerType(), PandasUDFType.SCALAR)
def pd_fuzz_partial_token_sort_ratio(col1, col2):
    """ Calculate "partial token sort" ratio (`fuzz.partial_token_sort_ratio`) between two text columns.

    Args:
        col1 (Spark Column): 1st text column
        col2 (Spark Column): 2nd text column

    Returns:
        Spark Column (IntegerType): result of `fuzz.partial_token_sort_ratio` calculation.
    """
    return pd.Series(map(fuzz.partial_token_sort_ratio, col1.astype(str), col2.astype(str)))

# TODO: `process` function


# noinspection PyArgumentList
@pandas_udf(IntegerType(), PandasUDFType.SCALAR)
def pd_damerau_levenshtein_distance(col1, col2):
    """ Calculate Damerau Levenshtein distance between two text columns.

    Args:
        col1 (Spark Column): 1st text column
        col2 (Spark Column): 2nd text column

    Returns:
        Spark Column (IntegerType): with Damerau Levenshtein distances.
    """
    return pd.Series(map(jellyfish.damerau_levenshtein_distance, col1.astype(str), col2.astype(str)))


# jellyfish.hamming_distance
# noinspection PyArgumentList
@pandas_udf(IntegerType(), PandasUDFType.SCALAR)
def pd_hamming_distance(col1, col2):
    """ Calculate hamming distance between two text columns.

    Args:
        col1 (Spark Column): 1st text column
        col2 (Spark Column): 2nd text column

    Returns:
        Spark Column (IntegerType): with hamming distances.
    """
    return pd.Series(map(jellyfish.hamming_distance, col1.astype(str), col2.astype(str)))


# noinspection PyArgumentList
@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def pd_jaro_distance(col1, col2):
    """ Calculate `jaro_distance` between two text columns.

    Args:
        col1 (Spark Column): 1st text column
        col2 (Spark Column): 2nd text column

    Returns:
        Spark Column (DoubleType): with Jaro distances.
    """
    return pd.Series(map(jellyfish.jaro_distance, col1.astype(str), col2.astype(str)))


# noinspection PyArgumentList
@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def pd_jaro_winkler(col1, col2):
    """ Calculate `jellyfish.jaro_winkler` between two text columns.

    Args:
        col1 (Spark Column): 1st text column
        col2 (Spark Column): 2nd text column

    Returns:
        Spark Column (DoubleType): with Jaro Winkler scores.
    """
    return pd.Series(map(jellyfish.jaro_winkler, col1.astype(str), col2.astype(str)))


# noinspection PyArgumentList
@pandas_udf(StringType(), PandasUDFType.SCALAR)
def pd_match_rating_codex(target_col):
    """ Apply `jellyfish.match_rating_codex` to text column.

    Args:
        target_col (Spark Column): text column.

    Returns:
        Spark Column (StringType): with match rating codex.
    """
    return pd.Series(target_col.apply(lambda x: jellyfish.match_rating_codex(str(x))))


# noinspection PyArgumentList
@pandas_udf(BooleanType(), PandasUDFType.SCALAR)
def pd_match_rating_comparison(col1, col2):
    """ Calculate `jellyfish.match_rating_comparison` between two text columns.

    Args:
        col1 (Spark Column): 1st text column
        col2 (Spark Column): 2nd text column

    Returns:
        Spark Column (BooleanType, nullable): True / False / None matching results.
    """
    return pd.Series(map(jellyfish.match_rating_comparison, col1.astype(str), col2.astype(str)))


# noinspection PyArgumentList
@pandas_udf(StringType(), PandasUDFType.SCALAR)
def pd_metaphone(target_col):
    """ Apply `jellyfish.metaphone` to text column.

    Args:
        target_col (Spark Column): text column.

    Returns:
        Spark Column (StringType): metaphone encodings.
    """
    return pd.Series(target_col.apply(lambda x: jellyfish.metaphone(str(x))))


# noinspection PyArgumentList
@pandas_udf(StringType(), PandasUDFType.SCALAR)
def pd_nysiis(target_col):
    """ Apply `jellyfish.nysiis` to text column.

    Args:
        target_col (Spark Column): text column.

    Returns:
        Spark Column (StringType): NYSIIS encodings.
    """
    return pd.Series(target_col.apply(lambda x: jellyfish.nysiis(str(x))))


# noinspection PyArgumentList
@pandas_udf(StringType(), PandasUDFType.SCALAR)
def pd_porter_stem(target_col):
    """ Apply `jellyfish.porter_stem` to text column.

    Args:
        target_col (Spark Column): text column.

    Returns:
        Spark Column (StringType): porter stems.
    """
    return pd.Series(target_col.apply(lambda x: jellyfish.porter_stem(str(x))))
