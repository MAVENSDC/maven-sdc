'''
Created on Sep 22, 2016

@author: bstaley
'''
import unittest
import sys
from maven_database.models import MavenStatus
from tests.maven_test_utilities import db_utils
from maven_status import job, MAVEN_SDC_COMPONENT, MAVEN_SDC_EVENTS, global_component_id


class TestStatusJobs(unittest.TestCase):

    def setUp(self):
        self.component_id = MAVEN_SDC_COMPONENT.DROPBOX

    def tearDown(self):
        db_utils.delete_data()

    def testJobRunNominal(self):
        testJob = job.StatusJob(component_id=self.component_id)

        global test_value
        test_value = 0

        def test_run(test_val):
            global test_value
            test_value = test_val

        testJob.run(test_run, {'test_val': 42})

        self.assertEqual(42, test_value)

        query = MavenStatus.query
        query = query.filter(MavenStatus.component_id == self.component_id.name)
        results = query.all()

        self.assertEqual(2, len(results))
        self.assertEqual(1, len([n for n in results if n.event_id == MAVEN_SDC_EVENTS.START.name]))
        self.assertEqual(1, len([n for n in results if n.event_id == MAVEN_SDC_EVENTS.SUCCESS.name]))

    def testJobRunException(self):
        testJob = job.StatusJob(component_id=self.component_id)

        def test_run(test_val):
            raise Exception('TEST EXCEPTION')

        testJob.run(test_run)

        with self.assertRaises(Exception):
            testJob.run(test_run,
                        propagate_exceptions=True)

    def testCronJobRun(self):
        testJob = job.StatusCronJob(component_id=self.component_id)

        def test_run(test_val):
            sys.stdout.write(test_val)

        std_test_len = job.StatusCronJob.STD_OUT_LEN * 2 - 1
        testJob.run(test_run, {'test_val': '*' * std_test_len})

        query = MavenStatus.query
        query = query.filter(MavenStatus.component_id == self.component_id.name)
        results = query.all()

        self.assertEqual(3, len(results))
        self.assertEqual(1, len([n for n in results if n.event_id == MAVEN_SDC_EVENTS.START.name]))
        self.assertEqual(1, len([n for n in results if n.event_id == MAVEN_SDC_EVENTS.SUCCESS.name]))
        self.assertEqual(1, len([n for n in results if n.event_id == MAVEN_SDC_EVENTS.OUTPUT.name]))

        output_status = [n for n in results if n.event_id == MAVEN_SDC_EVENTS.OUTPUT.name][0]
        self.assertEqual('*' * std_test_len, output_status.description)

    def testCronJobLongStdoutRun(self):
        testJob = job.StatusCronJob(component_id=self.component_id)

        def test_run(test_val):
            sys.stdout.write(test_val)

        std_test_len = job.StatusCronJob.STD_OUT_LEN * 20
        testJob.run(test_run, {'test_val': '*' * std_test_len})

        query = MavenStatus.query
        query = query.filter(MavenStatus.component_id == self.component_id.name)
        results = query.all()

        output_status = [n for n in results if n.event_id == MAVEN_SDC_EVENTS.OUTPUT.name][0]
        stdout_max_len = job.StatusCronJob.STD_OUT_LEN * 2 + 5  # 5 is \n...\n
        self.assertTrue(len(output_status.description) <= stdout_max_len)

        self.assertIn('...', output_status.description)

    def testStatusJobContextManagement(self):
        component_enum = [MAVEN_SDC_COMPONENT.DROPBOX,
                          MAVEN_SDC_COMPONENT.FULL_INDEXER,
                          MAVEN_SDC_COMPONENT.DELTA_INDEXER,
                          MAVEN_SDC_COMPONENT.PDS_ARCHIVER,
                          MAVEN_SDC_COMPONENT.KP_INGESTER,
                          MAVEN_SDC_COMPONENT.ANC_INGESTER,
                          MAVEN_SDC_COMPONENT.L0_INGESTER,
                          MAVEN_SDC_COMPONENT.SPICE_INGESTER,
                          MAVEN_SDC_COMPONENT.NEW_MONITOR,
                          MAVEN_SDC_COMPONENT.QL_MONITOR,
                          MAVEN_SDC_COMPONENT.AUDITOR,
                          MAVEN_SDC_COMPONENT.ORBIT,
                          MAVEN_SDC_COMPONENT.WEB_MONITOR,
                          MAVEN_SDC_COMPONENT.DISK_CLEANER]
        for id_enum in component_enum:
            with job.StatusJob(component_id=id_enum) as job_test:
                self.assertEqual(id_enum, global_component_id[-1])
        self.assertEqual(global_component_id, [])

    def testMulitpleComponent(self):
       '''If more than one component_id is given'''
       component_enum = [MAVEN_SDC_COMPONENT.DROPBOX, MAVEN_SDC_COMPONENT.FULL_INDEXER]
       with job.StatusJob(component_id=component_enum[0]) as job_out:
           self.assertEqual([MAVEN_SDC_COMPONENT.DROPBOX], global_component_id) 
           with job.StatusJob(component_id=component_enum[1]) as job_in:
               self.assertEqual(component_enum, global_component_id)
               self.assertEqual(MAVEN_SDC_COMPONENT.FULL_INDEXER, global_component_id[1])
           self.assertEqual([MAVEN_SDC_COMPONENT.DROPBOX], global_component_id)
       self.assertEqual(global_component_id, [])
