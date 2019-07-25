import unittest
from .general_udfs_base_functions import (clean_string, empty_string_to_null, map_booleans_ynu,
                                          string_to_double_pfd, string_to_double_cfd)


class TestGeneralUDFBaseFunctions(unittest.TestCase):

    def test_clean_string(self):
        assert clean_string('\u0000') is None
        assert clean_string(None) is None
        assert clean_string('') is None
        assert clean_string('abc ') == 'abc'
        assert clean_string('abc 123\u0000') == 'abc 123'

    def test_empty_string_to_null(self):
        assert empty_string_to_null('\u0000') is None
        # noinspection PyTypeChecker
        assert empty_string_to_null(None) is None
        assert empty_string_to_null('') is None
        assert empty_string_to_null('abc ') == 'abc '
        assert empty_string_to_null('abc 123\u0000') == 'abc 123\u0000'

    def test_map_booleans_ynu(self):
        # `N`
        assert map_booleans_ynu(False) == 'N'
        assert map_booleans_ynu(0) == 'N'
        assert map_booleans_ynu('0') == 'N'
        assert map_booleans_ynu('f') == 'N'
        assert map_booleans_ynu('F') == 'N'
        assert map_booleans_ynu('false') == 'N'
        assert map_booleans_ynu('False') == 'N'
        assert map_booleans_ynu('FALSE') == 'N'
        assert map_booleans_ynu('n') == 'N'
        assert map_booleans_ynu('N') == 'N'
        assert map_booleans_ynu('no') == 'N'
        assert map_booleans_ynu('No') == 'N'
        assert map_booleans_ynu('NO') == 'N'
        # `Y`
        assert map_booleans_ynu(True) == 'Y'
        assert map_booleans_ynu(1) == 'Y'
        assert map_booleans_ynu('1') == 'Y'
        assert map_booleans_ynu('t') == 'Y'
        assert map_booleans_ynu('T') == 'Y'
        assert map_booleans_ynu('true') == 'Y'
        assert map_booleans_ynu('True') == 'Y'
        assert map_booleans_ynu('TRUE') == 'Y'
        assert map_booleans_ynu('y') == 'Y'
        assert map_booleans_ynu('Y') == 'Y'
        assert map_booleans_ynu('yes') == 'Y'
        assert map_booleans_ynu('Yes') == 'Y'
        assert map_booleans_ynu('YES') == 'Y'
        # `Unknown`
        assert map_booleans_ynu('') == 'Unknown'
        assert map_booleans_ynu(' ') == 'Unknown'
        assert map_booleans_ynu(3) == 'Unknown'
        assert map_booleans_ynu(3.0) == 'Unknown'
        assert map_booleans_ynu(None) == 'Unknown'
        assert map_booleans_ynu('foo') == 'Unknown'
        assert map_booleans_ynu('BAR') == 'Unknown'

    def test_string_to_double(self):
        assert string_to_double_pfd("100") == 100.00
        assert string_to_double_pfd("100") == 100.00
        assert string_to_double_pfd("-100") == -100.00
        assert string_to_double_pfd("(100)") == -100.00
        assert string_to_double_pfd("$100") == 100.00
        assert string_to_double_pfd("-$100") == -100.00
        assert string_to_double_pfd("($100)") == -100.00
        assert string_to_double_pfd("100%") == 100.00
        assert string_to_double_pfd("-100%") == -100.00
        assert string_to_double_pfd("(100%)") == -100.00
        assert string_to_double_pfd("100.00") == 100.00
        assert string_to_double_pfd("-100.00") == -100.00
        assert string_to_double_pfd("(100.00)") == -100.00
        assert string_to_double_pfd("$100.00") == 100.00
        assert string_to_double_pfd("-$100.00") == -100.00
        assert string_to_double_pfd("($100.00)") == -100.00
        assert string_to_double_pfd("100.00%") == 100.00
        assert string_to_double_pfd("-100.00%") == -100.00
        assert string_to_double_pfd("(100.00%)") == -100.00
        #
        assert string_to_double_pfd("100 Apples") == 100.00
        assert string_to_double_pfd("$3.14/lbs.") == 3.14
        #
        assert string_to_double_cfd("4 294 967 295,000") == 4294967295.00
        assert string_to_double_cfd("4 294 967.295,000") == 4294967295.00
        assert string_to_double_cfd("4.294.967.295,000") == 4294967295.00
