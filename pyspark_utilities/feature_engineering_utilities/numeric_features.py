import itertools
import re
from pyspark.sql.functions import col


def generate_numeric_combination_features(df, numeric_fields=None, numeric_combos=None):
    """ Generate features by combining numeric fields (pairs).
        > sum (`__PLUS_`)
        > difference (`__MINUS__`)
        > product (`__PRODUCT__`)
        > quotient (`__QUOTIENT__`): gets quotient in both directions

    Args:
        df (Spark DataFrame): containing numeric feature columns
        numeric_fields (list): optional, provided subset of numeric fields to combine. Useful for already wide datasets.
        numeric_combos (list): optional, provided subset of numeric combinations. Useful for very wide datasets.

    Returns:
        Spark DataFrame: containing pair-wise numeric combination features.
    """
    if numeric_combos is None:
        if numeric_fields is None:
            numeric_fields = [x[0] for x in df.dtypes if
                              x[1] in ['double', 'int', 'bigint'] or re.match('^decimal.*', x[1])]
        numeric_combos = list(itertools.combinations(numeric_fields, 2))

    df_with_plus_features = (df
                             .select(*((col(pair[0]) + col(pair[1])).alias(pair[0] + '__PLUS__' + pair[1])
                                       if pair in numeric_combos
                                       else pair
                                       for pair in df.columns + numeric_combos)))

    df_with_minus_features = (df_with_plus_features
                              .select(*((col(pair[0]) - col(pair[1])).alias(pair[0] + '__MINUS__' + pair[1])
                                        if pair in numeric_combos
                                        else pair
                                        for pair in df_with_plus_features.columns + numeric_combos)))

    df_with_product_features = (df_with_minus_features
                                .select(*((col(pair[0]) * col(pair[1])).alias(pair[0] + '__PRODUCT__' + pair[1])
                                          if pair in numeric_combos
                                          else pair
                                          for pair in df_with_minus_features.columns + numeric_combos)))

    df_with_quotient_features = (df_with_product_features
                                 .select(*((col(pair[0]) / col(pair[1])).alias(pair[0] + '__QUOTIENT__' + pair[1])
                                           if pair in numeric_combos
                                           else pair
                                           for pair in df_with_product_features.columns + numeric_combos)))

    df_with_rev_quotient_features = (df_with_quotient_features
                                     .select(*((col(pair[1]) / col(pair[0])).alias(pair[0] + '__QUOTIENT__' + pair[1])
                                               if pair in numeric_combos
                                               else pair
                                               for pair in df_with_quotient_features.columns + numeric_combos)))

    return df_with_rev_quotient_features
