'''
Unit tests for the MavenDbLogHandler
Created on Jun 22, 2015

@author: bstaley
'''
import os
import unittest
import logging
from maven_utilities import constants, time_utilities, maven_log
os.environ[constants.python_env] = 'testing'
from maven_database.models import MavenLog
from tests.maven_test_utilities import db_utils

logger = logging.getLogger('maven.maven_database.tests.db_log')


class TestDbLogger(unittest.TestCase):

    def setUp(self):
        maven_log.config_logging()

    def tearDown(self):
        db_utils.delete_data(MavenLog)

    def testDbLogFormat(self):
        '''Test that string formats are handled.'''
        val_sub = 42,
        string_sub = 'Abby'
        iso_time_sub = time_utilities.utc_now()
        logger.info('This is a formatted log %s,%s,%s', val_sub, string_sub, 'current time is %s' % iso_time_sub)
        log = MavenLog.query.first()
        self.assertIn(str(val_sub), log.message)
        self.assertIn(string_sub, log.message)
        self.assertIn(str(iso_time_sub), log.message)
        string_rep = MavenLog(log.logger, log.level, log.message, iso_time_sub)
        self.assertEqual(str(string_rep), "%s:\t%s\t%s\t%s" % (iso_time_sub, log.logger, log.level, log.message))
        