from maven_in_situ_kp_file_ingester import utilities
import os
import unittest
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from maven_database.models import MavenLog
from tests.maven_test_utilities.db_utils import delete_data
from maven_utilities import maven_log, time_utilities


class MavenInSituKpFileIngesterDbLoggingTestCase(unittest.TestCase):
    '''Tests of the maven_in_situ_kp_file_ingester's logging to the database.'''

    def setUp(self):
        maven_log.config_logging()

    def tearDown(self):
        delete_data(MavenLog)

    def test_logging_to_database(self):
        test_message = 'test logging_to_database message'
        count = MavenLog.query.count()
        self.assertEqual(count, 0)
        utilities.logger.info(test_message)
        count = MavenLog.query.count()
        self.assertEqual(count, 1)
        log = MavenLog.query.first()
        self.assertEqual(log.logger, 'maven.maven_in_situ_kp_file_ingester.utilities.db_log')
        self.assertEqual(log.level, 'INFO')
        self.assertIn(test_message, log.message)
        # Need to remove tz for sqllite
        t = time_utilities.utc_now().replace(tzinfo=None)
        tdiff = t - log.created_at
        self.assertTrue(tdiff.seconds < 5)
