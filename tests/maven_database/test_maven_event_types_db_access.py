import os
import unittest
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
import maven_database
from maven_database import db_session
from maven_database.models import MavenEventType


class MavenEventTypeDbAccessTestCase(unittest.TestCase):
    '''Tests access of the database table that holds the science
    files metadata.
    '''

    def setUp(self):
        pass

    def tearDown(self):
        for t in MavenEventType.query.all():
            db_session.delete(t)
        db_session.commit()

    def test_for_smoke(self):
        count = MavenEventType.query.count()
        self.assertTrue(count is not None)

    def test_insert(self):
        '''Test that string formats are handled'''
        assert MavenEventType.query.count() == 0, 'there were event types before test_insert ran'
        t = MavenEventType('test event',
                           True,
                           'test description',
                           'test discussion')
        maven_database.db_session.add(t)
        maven_database.db_session.commit()
        after_count = MavenEventType.query.count()
        self.assertEqual(after_count, 1)
        t2 = MavenEventType.query.first()
        self.assertEqual(t2.name, 'test event')
        self.assertEqual(t2.is_discrete, True)
        self.assertEqual(t2.description, 'test description')
        self.assertEqual(t2.discussion, 'test discussion')
        string_rep = MavenEventType(t2.name,
                                    t2.is_discrete,
                                    t2.description,
                                    t2.discussion,
                                    1)
        self.assertEqual(str(string_rep), '%s %s' % (t2.name, t2.is_discrete))