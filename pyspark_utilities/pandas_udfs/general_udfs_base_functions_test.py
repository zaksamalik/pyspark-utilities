import unittest
from .general_udfs_base_functions import (clean_string, empty_string_to_null, map_booleans_ynu,
                                          string_to_double_pfd, string_to_double_cfd)


class TestGeneralUDFBaseFunctions(unittest.TestCase):

    def test_clean_string(self):
        self.assertIsNone(clean_string('\u0000'))
        self.assertIsNone(clean_string(None))
        self.assertIsNone(clean_string(''))
        self.assertEqual(clean_string('abc '), 'abc')
        self.assertEqual(clean_string('abc 123\u0000'), 'abc 123')

    def test_empty_string_to_null(self):
        self.assertIsNone(empty_string_to_null('\u0000'))
        # noinspection PyTypeChecker
        self.assertIsNone(empty_string_to_null(None))
        self.assertIsNone(empty_string_to_null(''))
        self.assertEqual(empty_string_to_null('abc '), 'abc ')
        self.assertEqual(empty_string_to_null('abc 123\u0000'), 'abc 123\u0000')

    def test_map_booleans_ynu(self):
        # `N`
        self.assertEqual(map_booleans_ynu(False), 'N')
        self.assertEqual(map_booleans_ynu(0), 'N')
        self.assertEqual(map_booleans_ynu('0'), 'N')
        self.assertEqual(map_booleans_ynu('f'), 'N')
        self.assertEqual(map_booleans_ynu('F'), 'N')
        self.assertEqual(map_booleans_ynu('false'), 'N')
        self.assertEqual(map_booleans_ynu('False'), 'N')
        self.assertEqual(map_booleans_ynu('FALSE'), 'N')
        self.assertEqual(map_booleans_ynu('n'), 'N')
        self.assertEqual(map_booleans_ynu('N'), 'N')
        self.assertEqual(map_booleans_ynu('no'), 'N')
        self.assertEqual(map_booleans_ynu('No'), 'N')
        self.assertEqual(map_booleans_ynu('NO'), 'N')
        # `Y`
        self.assertEqual(map_booleans_ynu(True), 'Y')
        self.assertEqual(map_booleans_ynu(1), 'Y')
        self.assertEqual(map_booleans_ynu('1'), 'Y')
        self.assertEqual(map_booleans_ynu('t'), 'Y')
        self.assertEqual(map_booleans_ynu('T'), 'Y')
        self.assertEqual(map_booleans_ynu('true'), 'Y')
        self.assertEqual(map_booleans_ynu('True'), 'Y')
        self.assertEqual(map_booleans_ynu('TRUE'), 'Y')
        self.assertEqual(map_booleans_ynu('y'), 'Y')
        self.assertEqual(map_booleans_ynu('Y'), 'Y')
        self.assertEqual(map_booleans_ynu('yes'), 'Y')
        self.assertEqual(map_booleans_ynu('Yes'), 'Y')
        self.assertEqual(map_booleans_ynu('YES'), 'Y')
        # `Unknown`
        self.assertEqual(map_booleans_ynu(''), 'Unknown')
        self.assertEqual(map_booleans_ynu(' '), 'Unknown')
        self.assertEqual(map_booleans_ynu(3), 'Unknown')
        self.assertEqual(map_booleans_ynu(3.0), 'Unknown')
        self.assertEqual(map_booleans_ynu(None), 'Unknown')
        self.assertEqual(map_booleans_ynu('foo'), 'Unknown')
        self.assertEqual(map_booleans_ynu('BAR'), 'Unknown')

    def test_string_to_double(self):
        self.assertEqual(string_to_double_pfd("100"), 100.00)
        self.assertEqual(string_to_double_pfd("100"), 100.00)
        self.assertEqual(string_to_double_pfd("-100"), -100.00)
        self.assertEqual(string_to_double_pfd("(100)"), -100.00)
        self.assertEqual(string_to_double_pfd("$100"), 100.00)
        self.assertEqual(string_to_double_pfd("-$100"), -100.00)
        self.assertEqual(string_to_double_pfd("($100)"), -100.00)
        self.assertEqual(string_to_double_pfd("100%"), 100.00)
        self.assertEqual(string_to_double_pfd("-100%"), -100.00)
        self.assertEqual(string_to_double_pfd("(100%)"), -100.00)
        self.assertEqual(string_to_double_pfd("100.00"), 100.00)
        self.assertEqual(string_to_double_pfd("-100.00"), -100.00)
        self.assertEqual(string_to_double_pfd("(100.00)"), -100.00)
        self.assertEqual(string_to_double_pfd("$100.00"), 100.00)
        self.assertEqual(string_to_double_pfd("-$100.00"), -100.00)
        self.assertEqual(string_to_double_pfd("($100.00)"), -100.00)
        self.assertEqual(string_to_double_pfd("100.00%"), 100.00)
        self.assertEqual(string_to_double_pfd("-100.00%"), -100.00)
        self.assertEqual(string_to_double_pfd("(100.00%)"), -100.00)
        #
        self.assertEqual(string_to_double_pfd("100 Apples"), 100.00)
        self.assertEqual(string_to_double_pfd("$3.14/lbs."), 3.14)
        #
        self.assertEqual(string_to_double_cfd("4 294 967 295,000"), 4294967295.00)
        self.assertEqual(string_to_double_cfd("4 294 967.295,000"), 4294967295.00)
        self.assertEqual(string_to_double_cfd("4.294.967.295,000"), 4294967295.00)
