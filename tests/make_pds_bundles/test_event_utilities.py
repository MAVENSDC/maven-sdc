'''
Unit tests for PDS event utilities
Created on Aug 17, 2015

@author: bstaley
'''
import unittest
import datetime
import os
import pytz
from dateutil import parser

from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from make_pds_bundles import utilities
from maven_utilities import time_utilities
from tests.maven_test_utilities import db_utils
from maven_database.models import MavenEventType, MavenEvent, MavenEventTag
from maven_ops_database.models import OpsMissionEvent
from maven_ops_database.database import init_db, db_session as ops_db_session
from maven_database import db_session as maven_db_session

# Setup in-memory database
init_db()


class TestEventUtilities(unittest.TestCase):

    def setUp(self):
        # Insert some events into the database
        self.test_end = time_utilities.utc_now()
        _, self.test_events, _, _ = db_utils.create_maven_events(self.test_end,
                                                                 100,
                                                                 - 100,  # reverse chronological
                                                                 'TestType')
        self.test_ops_events = db_utils.insert_ops_events(start_time=self.test_end,
                                                          num_events=100,
                                                          delta_seconds=-100,  # reverse chronological
                                                          type_name='TestOpsType')

        self.maven_events_ordered = maven_db_session.query(MavenEvent).order_by(MavenEvent.start_time.asc()).all()
        self.ops_events_ordered = ops_db_session.query(OpsMissionEvent).order_by(OpsMissionEvent.starttime.asc()).all()

    def tearDown(self):
        db_utils.delete_data(MavenEventTag, MavenEvent, MavenEventType)
        db_utils.delete_ops_data()

    def testEventOrdering(self):
        event_results = utilities.query_for_events(datetime.datetime.min.replace(tzinfo=pytz.UTC), self.test_end)
        self.assertEqual(set(self.test_events), set(event_results))

        check_time = datetime.datetime.min.replace(tzinfo=pytz.UTC)

        for next_result in event_results:
            self.assertLess(check_time, next_result.start_time.replace(tzinfo=pytz.UTC))
            check_time = next_result.start_time.replace(tzinfo=pytz.UTC)

    def testOpsEventOrdering(self):
        event_results = utilities.query_for_ops_events(datetime.datetime.min.replace(tzinfo=pytz.UTC), self.test_end)
        self.assertEqual(set(self.test_ops_events), set(event_results))

        check_time = datetime.datetime.min.replace(tzinfo=pytz.UTC)

        for next_result in event_results:
            self.assertLess(check_time, next_result.starttime.replace(tzinfo=pytz.UTC))
            check_time = next_result.starttime.replace(tzinfo=pytz.UTC)

    def testTimeQueries(self):

        maven_event_results = utilities.query_for_events(self.maven_events_ordered[0].end_time + datetime.timedelta(seconds=1),
                                                         self.maven_events_ordered[-1].start_time - datetime.timedelta(seconds=1))

        self.assertEqual(self.maven_events_ordered[1:-1], maven_event_results)

        ops_event_results = utilities.query_for_ops_events(self.ops_events_ordered[0].endtime + datetime.timedelta(seconds=1),
                                                           self.ops_events_ordered[-1].starttime - datetime.timedelta(seconds=1))

        self.assertEqual(self.ops_events_ordered[1:-1], ops_event_results)

    def testSdcCsvGeneration(self):
        # determine csv format and ordering
        sdc_raw_results = [l for l in utilities.generate_sdc_events_csv(self.maven_events_ordered).getvalue().split('\n')]

        sdc_csv_results = [l.split(',') for l in sdc_raw_results if len(l) and l[0].isdigit()]

        sdc_csv_result_ids = [int(l[0]) for l in sdc_csv_results]

        # assert contents
        for _next in self.maven_events_ordered:
            self.assertIn(_next.id, sdc_csv_result_ids)

        self.assertEqual(len(sdc_csv_results), len(self.maven_events_ordered))

        # assert structure
        csv_headers = sdc_raw_results[0].strip().split(',')
        self.assertEqual('id', csv_headers[0])
        self.assertEqual('event_type_id', csv_headers[1])
        self.assertEqual('start_time', csv_headers[2])
        self.assertEqual('end_time', csv_headers[3])
        self.assertEqual('source', csv_headers[4])
        self.assertEqual('description', csv_headers[5])
        self.assertEqual('discussion', csv_headers[6])
        self.assertEqual('modified_time', csv_headers[7])

        # assert results are time ordered
        check_time = datetime.datetime.min.replace(tzinfo=pytz.UTC)
        for row in sdc_csv_results:
            next_time = parser.parse(row[2])
            self.assertGreaterEqual(next_time, check_time)
            check_time = next_time

    def testOpsCsvGeneration(self):
        # determine csv format and ordering
        ops_raw_results = [l for l in utilities.generate_ops_events_csv(self.ops_events_ordered).getvalue().split('\n')]

        ops_csv_results = [l.split(',') for l in ops_raw_results if len(l) and l[0].isdigit()]

        ops_csv_result_ids = [int(l[0]) for l in ops_csv_results]

        # assert contents
        for _next in self.ops_events_ordered:
            self.assertIn(_next.eventid, ops_csv_result_ids)

        self.assertEqual(len(ops_csv_results), len(self.ops_events_ordered))

        # assert structure
        csv_headers = ops_raw_results[0].strip().split(',')
        self.assertEqual('id', csv_headers[0])
        self.assertEqual('event_type_id', csv_headers[1])
        self.assertEqual('start_time', csv_headers[2])
        self.assertEqual('end_time', csv_headers[3])
        self.assertEqual('source', csv_headers[4])
        self.assertEqual('description', csv_headers[5])
        self.assertEqual('discussion', csv_headers[6])

        # assert results are time ordered
        check_time = datetime.datetime.min.replace(tzinfo=pytz.UTC)
        for row in ops_csv_results:
            next_time = parser.parse(row[2])
            self.assertGreaterEqual(next_time, check_time)
            check_time = next_time
