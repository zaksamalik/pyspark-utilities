from fuzzywuzzy import fuzz
import pandas as pd


def pd_fuzz_ratio(col1, col2):
    """ Calculate "simple" ratio (`fuzz.ratio`) on two text columns.

    Args:
        col1 (Spark Column): 1st text column
        col2 (Spark Column): 2nd text column

    Returns:
        Spark Column (int): result of `fuzz.ratio` calculation.
    """
    return pd.Series(map(fuzz.ratio, col1, col2))


def pd_fuzz_partial_ratio(col1, col2):
    """ Calculate "partial" ratio (`fuzz.partial_ratio`) on two text columns.

    Args:
        col1 (Spark Column): 1st text column
        col2 (Spark Column): 2nd text column

    Returns:
        Spark Column (int): result of `fuzz.partial_ratio` calculation.
    """
    return pd.Series(map(fuzz.partial_ratio, col1, col2))


def pd_fuzz_token_set_ratio(col1, col2):
    """ Calculate "token set" ratio (`fuzz.token_set_ratio`) on two text columns.

    Args:
        col1 (Spark Column): 1st text column
        col2 (Spark Column): 2nd text column

    Returns:
        Spark Column (int): result of `fuzz.token_set_ratio` calculation.
    """
    return pd.Series(map(fuzz.token_set_ratio, col1, col2))


def pd_fuzz_partial_token_set_ratio(col1, col2):
    """ Calculate "partial token set" ratio (`fuzz.partial_token_set_ratio`) on two text columns.

    Args:
        col1 (Spark Column): 1st text column
        col2 (Spark Column): 2nd text column

    Returns:
        Spark Column (int): result of `fuzz.partial_token_set_ratio` calculation.
    """
    return pd.Series(map(fuzz.partial_token_set_ratio, col1, col2))


def pd_fuzz_token_sort_ratio(col1, col2):
    """ Calculate "token sort" ratio (`fuzz.token_sort_ratio`) on two text columns.

    Args:
        col1 (Spark Column): 1st text column
        col2 (Spark Column): 2nd text column

    Returns:
        Spark Column (int): result of `fuzz.token_sort_ratio` calculation.
    """
    return pd.Series(map(fuzz.token_sort_ratio, col1, col2))


def pd_fuzz_partial_token_sort_ratio(col1, col2):
    """ Calculate "partial token sort" ratio (`fuzz.partial_token_sort_ratio`) on two text columns.

    Args:
        col1 (Spark Column): 1st text column
        col2 (Spark Column): 2nd text column

    Returns:
        Spark Column (int): result of `fuzz.partial_token_sort_ratio` calculation.
    """
    return pd.Series(map(fuzz.partial_token_sort_ratio, col1, col2))

# TODO: `process` function
