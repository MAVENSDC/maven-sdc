import os
import unittest

from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
import maven_database
from maven_database import db_session
from maven_database.models import MavenEventType
from maven_database.models import MavenEvent


class MavenEventDbAccessTestCase(unittest.TestCase):
    '''Tests access of the database table that holds the maven events.'''

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

    def tearDown(self):
        for t in MavenEventType.query.all():
            db_session.delete(t)
        for e in MavenEvent.query.all():
            db_session.delete(e)
        db_session.commit()

    def test_for_smoke(self):
        count = MavenEvent.query.count()
        self.assertTrue(count is not None)

    def test_insert(self):
        '''Test that string formats are handled'''
        assert MavenEvent.query.count() == 0, 'there were events before test_insert ran'
        utcnow = time_utilities.utc_now()
        event = MavenEvent(self.test_type.id,
                           utcnow,
                           utcnow,
                           'test source',
                           utcnow,
                           'test event description',
                           'test event discussion')
        maven_database.db_session.add(event)
        maven_database.db_session.commit()
        after_count = MavenEvent.query.count()
        self.assertEqual(after_count, 1)
        e2 = MavenEvent.query.first()
        self.assertEqual(e2.event_type_id, self.test_type.id)
        self.assertEqual(e2.start_time, utcnow.replace(tzinfo=None))  # sqlite can't store timezone information
        self.assertEqual(e2.end_time, utcnow.replace(tzinfo=None))
        self.assertEqual(e2.description, 'test event description')
        self.assertEqual(e2.discussion, 'test event discussion')

        # test relationship
        events = self.test_type.maven_events
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0], event)
        
        # test string representation of object
        string_rep = MavenEvent(e2.event_type_id,
                                e2.start_time,
                                e2.end_time,
                                'test source',
                                utcnow,
                                e2.description,
                                e2.discussion)
        self.assertEqual(str(string_rep), "%s %s %s %s" % (e2.event_type_id, e2.start_time, e2.end_time, e2.source))