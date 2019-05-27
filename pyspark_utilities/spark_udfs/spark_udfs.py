from pyspark.sql import Column, SparkSession
# noinspection PyUnresolvedReferences, PyProtectedMember
from pyspark.sql.column import _to_seq, _to_java_column


class SparkUDFs:
    def __init__(self, spark):
        """

        Args:
            spark (SparkSession): instantiated SparkSession
        """
        self.spark = spark

    def clean_string(self, target_col):
        """

        Args:
            target_col ():

        Returns:

        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _clean_string = sc._jvm.com.civicboost.spark.etl.utilities.GeneralUDFs.cleanString_UDF()
        return Column(_clean_string.apply(_to_seq(sc, [target_col], _to_java_column)))

    def empty_string_to_null(self, target_col):
        """

        Args:
            target_col ():

        Returns:

        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _empty_string_to_null = sc._jvm.com.civicboost.spark.etl.utilities.GeneralUDFs.emptyStringToNull_UDF()
        return Column(_empty_string_to_null.apply(_to_seq(sc, [target_col], _to_java_column)))

    def map_booleans_ynu(self, target_col):
        """

        Args:
            target_col ():

        Returns:

        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _map_booleans_ynu = sc._jvm.com.civicboost.spark.etl.utilities.GeneralUDFs.mapBooleansYNU_UDF()
        return Column(_map_booleans_ynu.apply(_to_seq(sc, [target_col], _to_java_column)))

    def string_to_double(self, target_col):
        """

        Args:
            target_col ():

        Returns:

        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _string_to_double = sc._jvm.com.civicboost.spark.etl.utilities.GeneralUDFs.stringToDouble_UDF()
        return Column(_string_to_double.apply(_to_seq(sc, [target_col], _to_java_column)))

    def string_is_number(self, target_col):
        """

        Args:
            target_col ():

        Returns:

        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _string_is_number = sc._jvm.com.civicboost.spark.etl.utilities.GeneralUDFs.stringIsNumber_UDF()
        return Column(_string_is_number.apply(_to_seq(sc, [target_col], _to_java_column)))

    def normalize_date_md(self, target_col):
        """

        Args:
            target_col ():

        Returns:

        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _normalize_date_md = sc._jvm.com.civicboost.spark.etl.utilities.DateTimeUDFs.normalizeDateMD_UDF()
        return Column(_normalize_date_md.apply(_to_seq(sc, [target_col], _to_java_column)))

    def normalize_date_dm(self, target_col):
        """

        Args:
            target_col ():

        Returns:

        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _normalize_date_dm = sc._jvm.com.civicboost.spark.etl.utilities.DateTimeUDFs.normalizeDateDM_UDF()
        return Column(_normalize_date_dm.apply(_to_seq(sc, [target_col], _to_java_column)))

    def normalize_timestamp_md(self, target_col):
        """

        Args:
            target_col ():

        Returns:

        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _normalize_timestamp_md = sc._jvm.com.civicboost.spark.etl.utilities.DateTimeUDFs.normalizeTimestampMD_UDF()
        return Column(_normalize_timestamp_md.apply(_to_seq(sc, [target_col], _to_java_column)))

    def normalize_timestamp_dm(self, target_col):
        """

        Args:
            target_col ():

        Returns:

        """
        sc = self.spark.sparkContext
        # noinspection PyUnresolvedReferences, PyProtectedMember
        _normalize_timestamp_dm = sc._jvm.com.civicboost.spark.etl.utilities.DateTimeUDFs.normalizeTimestampDM_UDF()
        return Column(_normalize_timestamp_dm.apply(_to_seq(sc, [target_col], _to_java_column)))
