import os
import unittest

from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
import maven_database
from maven_database import db_session
from maven_database.models import MavenEventType
from maven_database.models import MavenEvent
from maven_database.models import MavenEventTag


class MavenEventTagDbAccessTestCase(unittest.TestCase):
    '''Tests access of the database table that holds the MAVEN event tags.'''

    def setUp(self):
        assert MavenEventType.query.count() == 0, 'there were event types before test_insert ran'
        self.test_type = MavenEventType('test event',
                                        True,
                                        'test description',
                                        'test discussion')
        maven_database.db_session.add(self.test_type)
        maven_database.db_session.commit()
        after_count = MavenEventType.query.count()
        self.assertEqual(after_count, 1)
        assert MavenEvent.query.count() == 0, 'there were events before test_insert ran'
        utcnow = time_utilities.utc_now()
        self.event = MavenEvent(self.test_type.id,
                                utcnow,
                                utcnow,
                                'test source',
                                utcnow,
                                'test event description',
                                'test event discussion')
        maven_database.db_session.add(self.event)
        maven_database.db_session.commit()
        after_count = MavenEvent.query.count()
        self.assertEqual(after_count, 1)

    def tearDown(self):
        for t in MavenEventType.query.all():
            db_session.delete(t)
        for t in MavenEventTag.query.all():
            db_session.delete(t)
        for e in MavenEvent.query.all():
            db_session.delete(e)
        db_session.commit()

    def test_for_smoke(self):
        count = MavenEventTag.query.count()
        self.assertTrue(count is not None)

    def test_insert(self):
        '''Test that string formats are handled'''
        assert MavenEventTag.query.count() == 0, 'there were event tags before test_insert ran'
        tag = MavenEventTag(self.event.id,
                            'test tag')
        maven_database.db_session.add(tag)
        maven_database.db_session.commit()
        after_count = MavenEventTag.query.count()
        self.assertEqual(after_count, 1)
        t2 = MavenEventTag.query.first()
        self.assertEqual(t2.event_id, self.event.id)
        self.assertEqual(t2.tag, 'test tag')

        # test relationship
        self.assertEqual(len(self.event.tags), 1)
        et = self.event.tags[0]
        self.assertEqual(t2.tag, et.tag)

        # test string representation of object
        string_rep = MavenEventTag(t2.event_id,
                                   t2.tag)
        self.assertEqual(str(string_rep), '%d %s' % (t2.event_id, t2.tag))