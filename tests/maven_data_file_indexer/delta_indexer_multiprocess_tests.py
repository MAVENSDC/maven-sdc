'''
Created on Mar 30, 2016

@author: bstaley
'''
import os
import unittest
from time import sleep
from shutil import rmtree
import signal
from multiprocessing import Process, Queue as MultiQueue

from maven_utilities import constants
from maven_status import MAVEN_SDC_COMPONENT, MAVEN_SDC_EVENTS
os.environ[constants.python_env] = 'testing'
from maven_data_file_indexer import maven_delta_indexer
from tests.maven_test_utilities.file_system import get_temp_root_dir
from tests.maven_test_utilities.db_utils import delete_data
from maven_database.models import MavenStatus


class DeltaIndexerMultiProcessTests(unittest.TestCase):

    def setUp(self):

        self.test_root_dir = get_temp_root_dir()

    def tearDown(self):
        rmtree(self.test_root_dir)
        self.assertFalse(os.path.isdir(self.test_root_dir))
        delete_data()

    def testDeltaIndexerProcessAdd(self):

        test_queue = MultiQueue()

        def test_handler(event):
            test_queue.put(event)

        test_indexer = maven_delta_indexer.DeltaIndexer(base_directories=[self.test_root_dir],
                                                        delete_handlers=[test_handler],
                                                        close_handlers=[test_handler])

        test_process = Process(target=test_indexer.process_events)

        test_process.start()

        try:

            sleep(2)  # some time to register file watchers

            test_file = 'test_file.txt'
            file_filepath = os.path.join(self.test_root_dir, test_file)

            while test_queue.qsize() > 0:
                test_queue.get_nowait()  # ensure q is empty prior to test

            # test file add
            with open(file_filepath, 'w'):
                pass

            result_event = test_queue.get(timeout=2)
            self.assertEqual('IN_CLOSE_WRITE', result_event.maskname)
            self.assertEqual(file_filepath, result_event.pathname)

            while test_queue.qsize() > 0:
                test_queue.get_nowait()  # ensure q is empty prior to test

            # test file mod
            with open(file_filepath, 'w+') as test_fd:
                test_fd.write('filler')

            result_event = test_queue.get(timeout=5)
            self.assertEqual('IN_CLOSE_WRITE', result_event.maskname)
            self.assertEqual(file_filepath, result_event.pathname)

            sleep(2)  # some time for all IN_CLOSE_WRITE events to accumulate

            while test_queue.qsize() > 0:
                test_queue.get_nowait()  # ensure q is empty prior to test

            # test remove file
            os.remove(file_filepath)
            result_event = test_queue.get(timeout=2)
            self.assertEqual('IN_DELETE', result_event.maskname)
            self.assertEqual(file_filepath, result_event.pathname)

        finally:
            os.kill(test_process.pid, signal.SIGINT)  # interrupt to break off of file watch
            test_process.join(2)

    def testDeltaFileIndexerNominalTerminate(self):

        test_indexer = maven_delta_indexer.DeltaIndexer(base_directories=[self.test_root_dir])
        test_process = Process(target=test_indexer.process_events)

        test_process.start()
        sleep(2)  # some time to register file watchers

        # nominal term
        os.kill(test_process.pid, signal.SIGINT)
        test_process.join(1)
        self.assertEqual(1, MavenStatus.query.filter(MavenStatus.component_id == MAVEN_SDC_COMPONENT.DELTA_INDEXER.name)
                         .filter(MavenStatus.event_id == MAVEN_SDC_EVENTS.STATUS.name).count())
