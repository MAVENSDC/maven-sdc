'''
Created on Jan 6, 2015

@author: bstaley
'''
import unittest
from shutil import rmtree
import logging
import os
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'

from tests.maven_test_utilities import file_system


class TestExceptions(unittest.TestCase):

    def setUp(self):
        self.root_dir = file_system.get_temp_root_dir()
        self.test_file_loc = os.path.join(self.root_dir, 'maven_test_log.log')
        self.std_logger = logging.getLogger('test stdout')
        handler = logging.StreamHandler()
        self.std_logger.addHandler(handler)

        self.file_logger = logging.getLogger('test file')
        handler = logging.FileHandler(self.test_file_loc)
        self.file_logger.addHandler(handler)

    def tearDown(self):
        rmtree(self.root_dir)
        self.assertFalse(os.path.isdir(self.root_dir))

    def testSetupException(self):
        with self.assertRaises(KeyError):
            version = os.environ['MAVEN_VERSION']
