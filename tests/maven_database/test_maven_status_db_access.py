'''
Unit tests for the PdsArchiveRecord
Created on Feb 28, 2017

@author: cosc3564
'''
import os
import unittest
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
from maven_database.models import MavenStatus
from maven_database import db_session


class MavenStatusDbAccessTestCase(unittest.TestCase):
    
    def setUp(self):
        pass
    
    def tearDown(self):
        for row in MavenStatus.query.all():
            db_session.delete(row)
        db_session.commit()
    
    def test_for_smoke(self):
        count = MavenStatus.query.count()
        self.assertTrue(count is not None)
        
    def test_insert(self):
        '''Test that string formats are handled'''
        before_count = MavenStatus.query.count()
        date_timetag = time_utilities.utc_now()
        status = MavenStatus('component_id',
                             'event_id',
                             123,
                             'maven_status description',
                             date_timetag,
                             'maven_status summary')
        db_session.add(status)
        db_session.commit()
        after_count = MavenStatus.query.count()
        self.assertEqual(after_count, before_count + 1)
        
        m = MavenStatus.query.first()
        self.assertEqual(m.component_id, 'component_id')
        self.assertEqual(m.event_id, 'event_id')
        self.assertEqual(m.job_id, 123)
        self.assertEqual(m.description, 'maven_status description')
        self.assertEqual(m.timetag, date_timetag.replace(tzinfo=None))   # sqlite can't store timezone information
        self.assertEqual(m.summary, 'maven_status summary')
        
        string_rep = MavenStatus(m.component_id,
                                 m.event_id,
                                 m.job_id,
                                 m.description,
                                 m.timetag,
                                 m.summary)
        
        comparison = 'Component [%s] Event [%s] Job ID [%s] summary [%s] description [%s] timetag [%s]' % (m.component_id,
                                                                                                     m.event_id,
                                                                                                     m.job_id,
                                                                                                     m.summary,
                                                                                                     m.description,
                                                                                                     m.timetag)
        self.assertEqual(str(string_rep), comparison)
        