'''
Created on Oct 8, 2015
Unit test for the general regex in file_pattern module
@author: cosc3564
'''
import unittest
import os
from maven_utilities import constants
from maven_utilities import file_pattern
os.environ[constants.python_env] = 'testing'

class TestGeneralFilePattern(unittest.TestCase):

    def testAnalyzeGroupPattern(self):
        # Test no groups
        test_pattern = 'test.*'
        test_results = []
        file_pattern.analyze_group_pattern('test', test_pattern, results=test_results)

        self.assertEqual(1, len(test_results))
        self.assertTrue(test_results[0][2])

        test_results = []
        file_pattern.analyze_group_pattern('nomatch', test_pattern, results=test_results)

        self.assertFalse(test_results[0][2])

        # Test 1 group
        test_pattern = 'test_(grp1).*'
        test_results = []
        file_pattern.analyze_group_pattern('test_', test_pattern, results=test_results)

        self.assertEqual(2, len(test_results))
        self.assertFalse(test_results[0][2])
        self.assertTrue(test_results[1][2])

        test_results = []
        file_pattern.analyze_group_pattern('nomatch', test_pattern, results=test_results)

        self.assertEqual(2, len(test_results))
        self.assertFalse(test_results[0][2])
        self.assertFalse(test_results[1][2])

        # Test many groups
        test_pattern = 'test_(grp1)_(grp2)_(grp3).*'
        test_results = []
        file_pattern.analyze_group_pattern('test_grp1_grpN_grpM', test_pattern, results=test_results)

        self.assertEqual(3, len(test_results))
        self.assertFalse(test_results[0][2])
        self.assertFalse(test_results[1][2])
        self.assertTrue(test_results[2][2])

        # Test embedded group
        test_pattern = 'test_(grp(1))_(grp(2))_(grp(3)).*'
        test_results = []
        file_pattern.analyze_group_pattern('test_grp1_grpN_grpM', test_pattern, results=test_results)

        self.assertEqual(4, len(test_results))
        self.assertFalse(test_results[0][2])
        self.assertFalse(test_results[1][2])
        self.assertFalse(test_results[2][2])
        self.assertEqual('test_(grp(1))_(grp.*)_.*', test_results[3][1])
        self.assertTrue(test_results[3][2])
