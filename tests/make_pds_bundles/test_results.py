'''
Created on Sep 23, 2015

Unit tests for the make_pds_bundles.results package

@author: bstaley
'''
import unittest
import os

from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
from make_pds_bundles import results
from maven_database.models import PdsArchiveRecord
from tests.maven_test_utilities import db_utils


class TestPdsGenerationResults(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        db_utils.delete_data()

    def test_results_note_replace(self):
        '''Test ability to replace the note field in a pds archive result'''
        test_time = time_utilities.utc_now()
        test_notes = 'Notes prior to update'
        test_bundle_name = 'test_bundle'
        results.record_results(generation_time=test_time,
                               start_time=test_time,
                               end_time=test_time,
                               command_line='just a test',
                               configuration='just a test',
                               dry_run=False,
                               result_directory='/',
                               bundle_file_name=test_bundle_name,
                               manifest_file_name=None,
                               checksum_file_name=None,
                               result_version=None,
                               generation_result='TEST',
                               pds_status='TEST',
                               notes=test_notes)

        archive_result = PdsArchiveRecord.query.first()
        self.assertEqual(test_notes, archive_result.notes)

        test_notes_update = 'Some new Notes'
        results.update_result_notes(test_bundle_name, test_notes_update, False)

        archive_result = PdsArchiveRecord.query.all()
        self.assertEqual(1, len(archive_result))
        self.assertEqual(test_notes_update, archive_result[0].notes)

    def test_results_note_append(self):
        '''Test ability to append the note field in a pds archive result'''
        test_time = time_utilities.utc_now()
        test_notes = 'Notes prior to append'
        test_bundle_name = 'test_bundle'
        results.record_results(generation_time=test_time,
                               start_time=test_time,
                               end_time=test_time,
                               command_line='just a test',
                               configuration='just a test',
                               dry_run=False,
                               result_directory='/',
                               bundle_file_name=test_bundle_name,
                               manifest_file_name=None,
                               checksum_file_name=None,
                               result_version=None,
                               generation_result='TEST',
                               pds_status='TEST',
                               notes=test_notes)

        archive_result = PdsArchiveRecord.query.first()

        self.assertEqual(test_notes, archive_result.notes)

        test_notes_update = 'Some new Notes'
        results.update_result_notes(test_bundle_name, test_notes_update, True)

        archive_result = PdsArchiveRecord.query.all()
        self.assertEqual(1, len(archive_result))
        self.assertIn(test_notes, archive_result[0].notes)
        self.assertIn(test_notes_update, archive_result[0].notes)
        
        # Testing update_result_status if entry does not exist (logger.warning)
        self.assertIsNone(results.update_result_notes('', ''))

    def test_results_status_update(self):
        '''Test ability to update the status field in a pds archive result'''
        test_time = time_utilities.utc_now()
        test_status = 'status prior to update'
        test_bundle_name = 'test_bundle'

        results.record_results(generation_time=test_time,
                               start_time=test_time,
                               end_time=test_time,
                               command_line='just a test',
                               configuration='just a test',
                               dry_run=False,
                               result_directory='/',
                               bundle_file_name=test_bundle_name,
                               manifest_file_name=None,
                               checksum_file_name=None,
                               result_version=None,
                               generation_result='TEST',
                               pds_status=test_status,
                               notes='TEST')

        archive_result = PdsArchiveRecord.query.first()
        self.assertEqual(test_status, archive_result.pds_status)

        test_status_update = 'Some new status'
        results.update_result_status(test_bundle_name, test_status_update)
        
        archive_result = PdsArchiveRecord.query.all()
        self.assertEqual(1, len(archive_result))
        self.assertEqual(test_status_update, archive_result[0].pds_status)
        
        self.assertIsNone(results.update_result_status('', ''))

