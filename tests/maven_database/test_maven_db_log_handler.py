import os
import unittest
import logging
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
import maven_database
from maven_database.models import MavenLog
from maven_database.maven_db_log_handler import MavenDbLogHandler


class MavenDbLogHandlerTestCase(unittest.TestCase):
    '''Tests the MAVEN database log handler.'''

    def setUp(self):
        self.logger = logging.getLogger('tester')
        self.logger.setLevel(logging.DEBUG)
        self.db_handler = MavenDbLogHandler()
        self.db_handler.setLevel(logging.DEBUG)
        self.logger.addHandler(self.db_handler)

    def tearDown(self):
        rows = MavenLog.query.all()
        for r in rows:
            maven_database.db_session.delete(r)
        maven_database.db_session.commit()
        maven_database.db_session.close()
        self.logger.removeHandler(self.db_handler)

    def test_debug_logging(self):
        count = MavenLog.query.count()
        self.assertTrue(count is not None)
        self.assertEqual(count, 0)
        self.logger.debug('debug test')
        count = MavenLog.query.count()
        self.assertTrue(count is not None)
        self.assertEqual(count, 1)
        log = MavenLog.query.first()
        self.assertEqual(log.logger, 'tester')
        self.assertEqual(log.level, 'DEBUG')
        self.assertEqual(log.message, 'debug test')

    def test_info_logging(self):
        count = MavenLog.query.count()
        self.assertTrue(count is not None)
        self.assertEqual(count, 0)
        self.logger.info('info test')
        log = MavenLog.query.first()
        count = MavenLog.query.count()
        self.assertTrue(count is not None)
        self.assertEqual(count, 1)
        self.assertEqual(log.logger, 'tester')
        self.assertEqual(log.level, 'INFO')
        self.assertEqual(log.message, 'info test')

    def test_warning_logging(self):
        count = MavenLog.query.count()
        self.assertTrue(count is not None)
        self.assertEqual(count, 0)
        self.logger.warning('warning test')
        count = MavenLog.query.count()
        self.assertTrue(count is not None)
        self.assertEqual(count, 1)
        log = MavenLog.query.first()
        self.assertEqual(log.logger, 'tester')
        self.assertEqual(log.level, 'WARNING')
        self.assertEqual(log.message, 'warning test')

    def test_error_logging(self):
        count = MavenLog.query.count()
        self.assertTrue(count is not None)
        self.assertEqual(count, 0)
        self.logger.error('error test')
        log = MavenLog.query.first()
        self.assertEqual(log.logger, 'tester')
        self.assertEqual(log.level, 'ERROR')
        self.assertEqual(log.message, 'error test')

    def test_critical_logging(self):
        count = MavenLog.query.count()
        self.assertTrue(count is not None)
        self.assertEqual(count, 0)
        self.logger.critical('critical test')
        log = MavenLog.query.first()
        self.assertEqual(log.logger, 'tester')
        self.assertEqual(log.level, 'CRITICAL')
        self.assertEqual(log.message, 'critical test')
