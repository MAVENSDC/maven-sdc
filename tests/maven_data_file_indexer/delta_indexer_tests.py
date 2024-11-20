'''
Created on Mar 30, 2016

@author: bstaley
'''
import os
import unittest
from pyinotify import Event
from shutil import rmtree
from datetime import timedelta
from multiprocessing import Queue

from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
from maven_database.models import MavenStatus
from maven_status import MAVEN_SDC_COMPONENT, MAVEN_SDC_EVENTS
from maven_data_file_indexer import maven_delta_indexer, index_worker
from tests.maven_test_utilities.file_system import get_temp_root_dir
from tests.maven_test_utilities.db_utils import delete_data


class DeltaIndexerTests(unittest.TestCase):

    def setUp(self):

        self.test_root_dir = get_temp_root_dir()
        # no need for multiproc queues for these tests.  There is a significant overhead for
        # queue IPC that needs to be cared for if you use multiproc queues.
        maven_delta_indexer.work_queue = Queue()
        maven_delta_indexer.error_queue = Queue()

    def tearDown(self):
        rmtree(self.test_root_dir)
        self.assertFalse(os.path.isdir(self.test_root_dir))
        delete_data()

    def testOverflowHandler(self):
        test_event = Event({
            'mask': 0x00000001,
            'maskname': 'IN_ACCESS',
            'path': self.test_root_dir,
            'name': 'test_file.txt'})

        with self.assertRaises(KeyboardInterrupt):
            maven_delta_indexer.indexer_q_overflow_handler(test_event)
        self.assertEqual(1, MavenStatus.query.filter(MavenStatus.component_id == MAVEN_SDC_COMPONENT.DELTA_INDEXER.name)
                         .filter(MavenStatus.event_id == MAVEN_SDC_EVENTS.STATUS.name).count())

    def testCheckWorkerHandler(self):
        test_event = Event({
            'mask': 0x00000001,
            'maskname': 'IN_ACCESS',
            'path': self.test_root_dir,
            'name': 'test_file.txt'})
        maven_delta_indexer.error_queue.put('error')
        with self.assertRaises(RuntimeError):
            maven_delta_indexer.check_worker(test_event)
    
    def testRootIndexer(self):
        root_dir = self.test_root_dir
        self.assertTrue(os.path.isdir(root_dir))
        root_index = maven_delta_indexer.get_root_indexer([self.test_root_dir])
        self.assertEqual(("DeltaIndexer -  base_directory ['%s']" % root_dir), str(root_index))

    def testAddRemModHandlers(self):
        test_filename = 'test_file.txt'
        test_pathname = os.path.join(self.test_root_dir, test_filename)
        test_event = Event({
            'mask': 0x00000001,
            'maskname': 'IN_ACCESS',
            'path': self.test_root_dir,
            'name': test_filename,
            'pathname': test_pathname})

        maven_delta_indexer.indexer_close_handler(test_event)
        result_event = maven_delta_indexer.work_queue.get()
        self.assertEqual(test_pathname, result_event.full_filename)
        self.assertEqual(index_worker.FILE_EVENT.CLOSED, result_event.file_event)
        self.assertTrue((time_utilities.utc_now() - result_event.event_time) < timedelta(seconds=1))

        maven_delta_indexer.indexer_rem_handler(test_event)
        result_event = maven_delta_indexer.work_queue.get()
        self.assertEqual(test_pathname, result_event.full_filename)
        self.assertEqual(index_worker.FILE_EVENT.REMOVED, result_event.file_event)
        self.assertTrue((time_utilities.utc_now() - result_event.event_time) < timedelta(seconds=1))

        maven_delta_indexer.indexer_close_handler(test_event)
        result_event = maven_delta_indexer.work_queue.get()
        self.assertEqual(test_pathname, result_event.full_filename)
        self.assertEqual(index_worker.FILE_EVENT.CLOSED, result_event.file_event)
        self.assertTrue((time_utilities.utc_now() - result_event.event_time) < timedelta(seconds=1))

    def testINotifyHander(self):
        CLOSE_MASK = 0x00000001
        REM_MASK = 0x00000002
        Q_OF_MASK = 0x00000004

        global handlers_invoked
        handlers_invoked = 0x00000000

        def test_close(event):
            global handlers_invoked
            handlers_invoked |= CLOSE_MASK

        def test_rem(event):
            global handlers_invoked
            handlers_invoked |= REM_MASK

        def test_qof(event):
            global handlers_invoked
            handlers_invoked |= Q_OF_MASK

        test_inotif_handler = maven_delta_indexer.DeltaIndexer.INotifyHandler(delete_handlers=[test_rem],
                                                                              close_handlers=[test_close],
                                                                              q_overflow_handlers=[test_qof])

        test_event = Event({
            'mask': 0x00000001,
            'maskname': 'IN_ACCESS',
            'path': self.test_root_dir,
            'name': 'test_file.txt'})

        test_inotif_handler.process_IN_CLOSE_WRITE(test_event)
        self.assertTrue(handlers_invoked & CLOSE_MASK)
        test_inotif_handler.process_IN_DELETE(test_event)
        self.assertTrue(handlers_invoked & REM_MASK)
        test_inotif_handler.process_IN_Q_OVERFLOW(test_event)
        self.assertTrue(handlers_invoked & Q_OF_MASK)
