import holidays
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StringType


# noinspection PyArgumentList
@pandas_udf(StringType(), PandasUDFType.SCALAR)
def pd_is_holiday_usa(target_col):
    def is_holiday_usa(dt):
        if dt is None:
            return 'Unknown'
        elif pd.to_datetime(dt) in holidays.US():
            return 'Y'
        else:
            return 'N'

    return target_col.apply(lambda x: is_holiday_usa(x))
