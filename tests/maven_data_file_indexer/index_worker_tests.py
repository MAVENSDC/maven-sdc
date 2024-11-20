'''
Created on Mar 28, 2016

@author: bstaley
'''
import unittest
import os
from shutil import rmtree
import logging
import mock

from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from maven_utilities import time_utilities
from tests.maven_test_utilities import db_utils, file_system, log_handlers
from tests.maven_test_utilities import file_names
from maven_data_file_indexer import index_worker
from maven_data_file_indexer.index_worker import logger as idx_logger
from maven_database.models import ScienceFilesMetadata, AncillaryFilesMetadata


class IndexWorkerTests(unittest.TestCase):

    def setUp(self):
        self.assertEqual([], ScienceFilesMetadata.query.all())
        self.root_directory = file_system.get_temp_root_dir()
        self.test_handler = log_handlers.RecordHandler()
        idx_logger.addHandler(self.test_handler)

    def tearDown(self):
        rmtree(self.root_directory)
        self.assertFalse(os.path.isdir(self.root_directory))
        db_utils.delete_data(ScienceFilesMetadata, AncillaryFilesMetadata)
        self.test_handler.clear()
        idx_logger.removeHandler(self.test_handler)

    def testBadFileEvent(self):
        test_event = index_worker.FileEvent('does_not_matter.txt', -1, time_utilities.utc_now())

        index_worker.process_file_event(test_event)

        search_record = log_handlers.get_records(self.test_handler.records,
                                                 level=logging.WARNING,
                                                 msg_key_word='Invalid FILE_EVENT type')
        self.assertEqual(1, len(search_record))

    def testBadFileName(self):
        test_filename = 'bogus_filename.txt'
        file_system.build_test_files_and_structure(default_file_contents="filler",
                                                   files_base_dir=self.root_directory,
                                                   files_list=[test_filename])

        test_event = index_worker.FileEvent(os.path.join(self.root_directory, test_filename),
                                            index_worker.FILE_EVENT.CLOSED,
                                            time_utilities.utc_now())

        index_worker.process_file_updated_event(test_event)

        search_record = log_handlers.get_records(self.test_handler.records,
                                                 level=logging.WARNING,
                                                 msg_key_word='Unable to generate metadata for')
        self.assertEqual(1, len(search_record))

    def testFileEventsAdd(self):
        test_description = 'test-indexer-add-event'

        test_filename = file_names.generate_science_file_name(description=test_description)

        file_system.build_test_files_and_structure(default_file_contents="filler",
                                                   files_base_dir=self.root_directory,
                                                   files_list=[test_filename])

        test_event = index_worker.FileEvent(os.path.join(self.root_directory, test_filename),
                                            index_worker.FILE_EVENT.CLOSED,
                                            time_utilities.utc_now())

        index_worker.process_file_updated_event(test_event)

        result = ScienceFilesMetadata.query.first()
        self.assertEqual(test_description, result.descriptor)

    def testFileEventUpdate(self):
        test_description = 'test-indexer-update-event'
        test_filename = file_names.generate_science_file_name(description=test_description)

        db_utils.insert_science_files_metadata(file_name=test_filename,
                                               file_size=0)

        file_filler = 'x' * 1000
        file_system.build_test_files_and_structure(default_file_contents=file_filler,
                                                   files_base_dir=self.root_directory,
                                                   files_list=[test_filename])

        test_event = index_worker.FileEvent(os.path.join(self.root_directory, test_filename),
                                            index_worker.FILE_EVENT.CLOSED,
                                            time_utilities.utc_now())

        index_worker.process_file_updated_event(test_event)

        result = ScienceFilesMetadata.query.first()
        self.assertEqual(len(file_filler), result.file_size)

    def testFileEventRemove(self):
        test_description = 'test-indexer-remove-event'
        test_filename = file_names.generate_science_file_name(description=test_description)

        db_utils.insert_science_files_metadata(file_name=test_filename,
                                               file_size=0)

        file_system.build_test_files_and_structure(default_file_contents="filler",
                                                   files_base_dir=self.root_directory,
                                                   files_list=[test_filename])

        test_event = index_worker.FileEvent(os.path.join(self.root_directory, test_filename),
                                            index_worker.FILE_EVENT.REMOVED,
                                            time_utilities.utc_now())

        index_worker.process_file_removed_event(test_event)

        self.assertEqual([], ScienceFilesMetadata.query.all())

    def testFileEventRemoveNonExistent(self):
        test_description = 'test-indexer-remove-non-existent-event'
        test_filename = file_names.generate_science_file_name(description=test_description)

        test_event = index_worker.FileEvent(os.path.join(self.root_directory, test_filename),
                                            index_worker.FILE_EVENT.REMOVED,
                                            time_utilities.utc_now())

        index_worker.process_file_removed_event(test_event)

    # @unittest.skip('skipping')
    def testFileEventRemoveException(self):
        from multiprocessing import Queue
        in_q = Queue()  # use non-multiprocess queues
        out_q = Queue()

        # don't use the multiproc handler because exceptions aren't pickleable (traceback in particular)
        idx_logger.removeHandler(self.test_handler)
        test_filename = file_names.generate_science_file_name()
        in_q.put(index_worker.FileEvent(test_filename,
                                        index_worker.FILE_EVENT.REMOVED,
                                        time_utilities.utc_now()))

        with mock.patch('maven_data_file_indexer.utilities.delete_science_file_metadata_from_filename',
                        side_effect=self.throw_exception):
            index_worker.process_file_events(in_q, out_q)
        self.assertEqual(1, out_q.qsize())

    def throw_exception(self, args):
        raise Exception('dummy test exception')
