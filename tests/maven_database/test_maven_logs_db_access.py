import os
import unittest
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
import maven_database
from maven_database.models import MavenLog


class MavenLogDbAccessTestCase(unittest.TestCase):
    '''Tests access of the database table that holds the MAVEN
    dropbox manager log messages.
    '''

    def setUp(self):
        pass

    def tearDown(self):
        rows = MavenLog.query.all()
        for r in rows:
            maven_database.db_session.delete(r)
        maven_database.db_session.commit()
        maven_database.db_session.close()

    def test_for_smoke(self):
        count = MavenLog.query.count()
        self.assertTrue(count is not None)

    def test_insert(self):
        '''Test that string formats are handled'''
        before_count = MavenLog.query.count()
        iso_current_time = time_utilities.utc_now()
        m = MavenLog('test logger', 'test level', 'test message', iso_current_time)
        maven_database.db_session.add(m)
        maven_database.db_session.commit()
        after_count = MavenLog.query.count()
        self.assertEqual(after_count - 1, before_count)
        log = MavenLog.query.first()
        self.assertEqual(log.logger, 'test logger')
        self.assertEqual(log.level, 'test level')
        self.assertEqual(log.message, 'test message')
        self.assertTrue(log.created_at is not None)
        tdiff = iso_current_time.replace(tzinfo=None) - log.created_at   # sqlite can't store timezone information
        self.assertTrue(tdiff.seconds < 5)
