'''
Created on Apr 12, 2016

@author: bstaley
'''
import os
import unittest
from shutil import rmtree
import itertools

from maven_dropbox_mgr import utilities, config
from tests.maven_test_utilities.file_system import get_temp_root_dir


class TestDbmUtilities(unittest.TestCase):

    def setUp(self):
        self.root_source_directory = get_temp_root_dir()

        for levels in itertools.product(['a', 'b', 'c'], ['1', '2', '3'], ['a', 'b', 'c']):
            os.makedirs(os.path.join(self.root_source_directory, *levels))

    def tearDown(self):
        rmtree(self.root_source_directory)
        self.assertFalse(os.path.isdir(self.root_source_directory))

    def testFindInvalidLocTop(self):
        os.makedirs(os.path.join(self.root_source_directory, config.invalid_dir_name))
        result = utilities.find_dir_loc(os.path.join(self.root_source_directory, 'a', '2', 'a'),
                                        config.invalid_dir_name)
        self.assertIsNotNone(result)

    def testFindInvalidLocTopLimit(self):
        os.makedirs(os.path.join(self.root_source_directory, config.invalid_dir_name))
        result = utilities.find_dir_loc(os.path.join(self.root_source_directory, 'a', '2', 'a'),
                                        config.invalid_dir_name,
                                                self.root_source_directory)
        self.assertIsNotNone(result)

    def testFindInvalidLocLimit(self):
        os.makedirs(os.path.join(self.root_source_directory, config.invalid_dir_name))
        result = utilities.find_dir_loc(os.path.join(self.root_source_directory, 'a', '2', 'a'),
                                        config.invalid_dir_name,
                                                os.path.join(self.root_source_directory, 'a', '2', 'a'))
        self.assertIsNone(result)

    def testFindInvalidOutLimit(self):
        os.makedirs(os.path.join(self.root_source_directory, config.invalid_dir_name))
        result = utilities.find_dir_loc(os.path.join(self.root_source_directory, 'a', '2', 'a'),
                                        config.invalid_dir_name,
                                                os.path.join(self.root_source_directory, 'a', '3', 'a'))
        self.assertIsNone(result)
