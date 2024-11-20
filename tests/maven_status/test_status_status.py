'''
Created on Sep 22, 2016

@author: bstaley
'''
import datetime
import unittest
from maven_database.models import MavenStatus
from maven_utilities import  time_utilities
from tests.maven_test_utilities import db_utils
from maven_status import MAVEN_SDC_COMPONENT, MAVEN_SDC_EVENTS, status


class TestStatus(unittest.TestCase):

    def setUp(self):
        self.component_id = MAVEN_SDC_COMPONENT.DROPBOX
        self.event_id = MAVEN_SDC_EVENTS.SUCCESS
        self.job_id = 9876543210
        self.summary = 'TEST SUMMARY'
        self.description = 'TEST DESCRIPTION'
        self.time = time_utilities.utc_now().replace(tzinfo=None)  # sqlite can't store timezone info

    def tearDown(self):
        db_utils.delete_data()

    def testComponentIdNone(self):
       '''If component_id is not defined within with a relative or global context'''
       with self.assertRaises(ValueError):
            status.add_status(component_id=None,
                              event_id=self.event_id,
                              job_id=self.job_id,
                              summary=self.summary,
                              description=self.description,
                              timetag=self.time)

       with self.assertRaises(ValueError):
            status.add_exception_status(component_id=None,
                              event_id=self.event_id,
                              job_id=self.job_id,
                              summary=self.summary,
                              timetag=self.time)

    def testStatusNominal(self):

        status.add_status(component_id=self.component_id,
                          event_id=self.event_id,
                          job_id=self.job_id,
                          summary=self.summary,
                          description=self.description,
                          timetag=self.time)
        results = MavenStatus.query.all()

        self.assertEqual(1, len(results))

        test_status = results[0]

        self.assertEqual(self.component_id.name, test_status.component_id)
        self.assertEqual(self.event_id.name, test_status.event_id)
        self.assertEqual(self.job_id, test_status.job_id)
        self.assertEqual(self.summary, test_status.summary)
        self.assertEqual(self.description, test_status.description)
        self.assertEqual(self.time, test_status.timetag)

    def testStatusMissingTime(self):

        status.add_status(component_id=self.component_id,
                          event_id=self.event_id,
                          job_id=self.job_id,
                          summary=self.summary,
                          description=self.description)

        results = MavenStatus.query.all()

        self.assertEqual(1, len(results))

        test_status = results[0]

        self.assertLess(test_status.timetag, time_utilities.utc_now().replace(tzinfo=None))

    def testStatusException(self):

        test_exception_str = 'This is a test...this is only a test'
        try:
            raise Exception(test_exception_str)
        except:

            status.add_exception_status(component_id=self.component_id,
                                        event_id=self.event_id,
                                        job_id=self.job_id,
                                        summary=self.summary,
                                        timetag=self.time)
            results = MavenStatus.query.all()
    
            self.assertEqual(1, len(results))
    
            test_status = results[0]
    
            self.assertEqual(self.component_id.name, test_status.component_id)
            self.assertEqual(self.event_id.name, test_status.event_id)
            self.assertEqual(self.job_id, test_status.job_id)
            self.assertEqual(self.summary, test_status.summary)
            self.assertIn(test_exception_str, test_status.description)
            self.assertEqual(self.time, test_status.timetag)
