'''
Created on Jun 8, 2015

@author: tbussell
'''
import os
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
os.environ['LOG_NAME'] = 'TEST_NAME'
import unittest
import logging
from maven_utilities import maven_log


class MavenLogUnitTests(unittest.TestCase):

    def setUp(self):
        maven_log.config_logging()

    def test_get_logger(self):
        bogus = logging.getLogger('made_up_package')
        drop_box = logging.getLogger('maven.maven_dropbox_mgr.utilities.db_log')
        self.assertEqual('made_up_package', bogus.name)
        self.assertEqual(0, len(bogus.handlers))
        self.assertEqual('maven.maven_dropbox_mgr.utilities.db_log', drop_box.name)
        self.assertEqual('db_log', drop_box.handlers[0].name)
