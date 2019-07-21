from pyspark.sql import Column, SparkSession
# noinspection PyUnresolvedReferences, PyProtectedMember
from pyspark.sql.column import _to_seq, _to_java_column


class SparkUDFs:
    def __init__(self, spark):
        """

        Args:
            spark (SparkSession): instantiated SparkSession.
        """
        self.spark = spark

    def clean_string(self, target_col):
        """ Remove Java ISO control characters from, and trim, string.

        Args:
            target_col (Spark Column): target column to be cleaned.

        Returns:
            Spark Column (StringType): cleaned version of input column.
        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _clean_string = sc._jvm.com.civicboost.spark.etl.utilities.GeneralUDFs.cleanString_UDF()
        return Column(_clean_string.apply(_to_seq(sc, [target_col], _to_java_column)))

    def empty_string_to_null(self, target_col):
        """ Convert empty strings to nulls.

        Args:
            target_col (Spark Column): target column to convert.

        Returns:
            Spark Column (StringType): target column with empty values converted to nulls.
        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _empty_string_to_null = sc._jvm.com.civicboost.spark.etl.utilities.GeneralUDFs.emptyStringToNull_UDF()
        return Column(_empty_string_to_null.apply(_to_seq(sc, [target_col], _to_java_column)))

    def generate_uuid(self):
        """ Generate V4 UUID.

        Returns:
            Spark Column (StringType): containing v4 UUIDs.
        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _generate_uuid = sc._jvm.com.civicboost.spark.etl.utilities.GeneralUDFs.generateUUID_UDF()
        return Column(_generate_uuid.apply(_to_seq(sc, [], _to_java_column)))

    def map_booleans_ynu(self, target_col):
        """ Map boolean values to `Y`, `N`, `Unknown`

        Args:
            target_col (Spark Column): target column containing boolean values to map.

        Returns:
            Spark Column (StringType): mapped values (`Y`, `N`, `Unknown`)
        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _map_booleans_ynu = sc._jvm.com.civicboost.spark.etl.utilities.GeneralUDFs.mapBooleansYNU_UDF()
        return Column(_map_booleans_ynu.apply(_to_seq(sc, [target_col], _to_java_column)))

    def string_to_double_pfd(self, target_col):
        """ Convert string to doubles where period represents decimal places (`pfd`).

        Args:
            target_col (Spark Column): containing double values in string format.

        Returns:
            Spark Column (DoubleType): containing double values converted from strings.
        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _string_to_double = sc._jvm.com.civicboost.spark.etl.utilities.GeneralUDFs.stringToDoublePeriodForDecimal_UDF()
        return Column(_string_to_double.apply(_to_seq(sc, [target_col], _to_java_column)))

    def string_to_double_cfd(self, target_col):
        """ Convert string to doubles where commas represents decimal places (`cfd`).

        Args:
            target_col (Spark Column): containing double values in string format.

        Returns:
            Spark Column (DoubleType): containing double values converted from strings.
        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _string_to_double = sc._jvm.com.civicboost.spark.etl.utilities.GeneralUDFs.stringToDoubleCommaForDecimal_UDF()
        return Column(_string_to_double.apply(_to_seq(sc, [target_col], _to_java_column)))

    def string_is_number(self, target_col):
        """ Return boolean if string can be converted to a number.

        Args:
            target_col (Spark Column): containing string to check for convertability to number.

        Returns:
            Spark Column (BooleanType): whether string can converted to a number.
        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _string_is_number = sc._jvm.com.civicboost.spark.etl.utilities.GeneralUDFs.stringIsNumber_UDF()
        return Column(_string_is_number.apply(_to_seq(sc, [target_col], _to_java_column)))

    def normalize_date_md(self, target_col):
        """ Convert string to date where MONTH is BEFORE DAY.

        Args:
            target_col (Spark Column): containing strings representing dates.

        Returns:
            Spark Column (DateType): containing dates converted from strings.
        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _normalize_date_md = sc._jvm.com.civicboost.spark.etl.utilities.DateTimeUDFs.normalizeDateMD_UDF()
        return Column(_normalize_date_md.apply(_to_seq(sc, [target_col], _to_java_column)))

    def normalize_date_dm(self, target_col):
        """ Convert string to date where DAY is BEFORE MONTH.

        Args:
            target_col (Spark Column): containing strings representing dates.

        Returns:
            Spark Column (DateType): containing dates converted from strings.
        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _normalize_date_dm = sc._jvm.com.civicboost.spark.etl.utilities.DateTimeUDFs.normalizeDateDM_UDF()
        return Column(_normalize_date_dm.apply(_to_seq(sc, [target_col], _to_java_column)))

    def normalize_timestamp_md(self, target_col):
        """ Convert string to timestamp where MONTH is BEFORE DAY.

        Args:
            target_col (Spark Column): containing strings representing timestamps.

        Returns:
            Spark Column (TimestampType): containing timestamps converted from strings.
        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _normalize_timestamp_md = sc._jvm.com.civicboost.spark.etl.utilities.DateTimeUDFs.normalizeTimestampMD_UDF()
        return Column(_normalize_timestamp_md.apply(_to_seq(sc, [target_col], _to_java_column)))

    def normalize_timestamp_dm(self, target_col):
        """ Convert string to timestamp where DAY is BEFORE MONTH.

        Args:
            target_col (Spark Column): containing strings representing timestamps.

        Returns:
            Spark Column (TimestampType): containing timestamps converted from strings.
        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _normalize_timestamp_dm = sc._jvm.com.civicboost.spark.etl.utilities.DateTimeUDFs.normalizeTimestampDM_UDF()
        return Column(_normalize_timestamp_dm.apply(_to_seq(sc, [target_col], _to_java_column)))
