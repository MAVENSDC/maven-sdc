import os
import unittest
import pytz
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
import maven_database
from maven_database.models import MavenDropboxMgrMove


class MavenDropboxMgrMoveDbAccessTestCase(unittest.TestCase):
    '''Tests access of the database table that holds the MAVEN
    dropbox manager file moves.
    '''

    def setUp(self):
        pass

    def tearDown(self):
        rows = MavenDropboxMgrMove.query.all()
        for r in rows:
            maven_database.db_session.delete(r)
        maven_database.db_session.commit()
        maven_database.db_session.close()

    def test_for_smoke(self):
        count = MavenDropboxMgrMove.query.count()
        self.assertTrue(count is not None)

    def test_insert(self):
        '''Test that string formats are handled'''
        before_count = MavenDropboxMgrMove.query.count()
        utcnow = time_utilities.utc_now()
        t = MavenDropboxMgrMove(utcnow, 'src/test_file', 'dest/test_file', 'fake_md5', 0)
        maven_database.db_session.add(t)
        maven_database.db_session.commit()
        after_count = MavenDropboxMgrMove.query.count()
        self.assertEqual(after_count - 1, before_count)
        m2 = MavenDropboxMgrMove.query.first()
        self.assertEqual(m2.when_moved.replace(tzinfo=pytz.UTC), utcnow)  # sqlite can't store timezone information
        self.assertEqual(m2.src_filename, 'src/test_file')
        self.assertEqual(m2.dest_filename, 'dest/test_file')
        self.assertEqual(m2.md5, 'fake_md5')
        self.assertEqual(m2.file_size, 0)
        # Add timezone back in since MavenDropboxMgrMove expects a timezone
        m2.when_moved = m2.when_moved.replace(tzinfo=pytz.UTC)
        string_rep = MavenDropboxMgrMove(m2.when_moved, m2.src_filename, m2.dest_filename, m2.md5, m2.file_size)
        self.assertEqual(str(string_rep), "%s %s %s" % (str(m2.when_moved), m2.src_filename, m2.dest_filename))