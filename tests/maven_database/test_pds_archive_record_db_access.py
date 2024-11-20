'''
Unit tests for the PdsArchiveRecord
Created on Feb 28, 2017

@author: cosc3564
'''
import os
import unittest
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
from maven_database.models import PdsArchiveRecord
from maven_database import db_session
from shutil import rmtree
from tests.maven_test_utilities import file_system


class PdsArchiveRecordDbAccessTestCase(unittest.TestCase):
    '''Tests access of the database table that holds the pds archive record
    files
    '''
    
    def setUp(self):
        self.directory_path = file_system.get_temp_root_dir()
        
    def tearDown(self):
        rmtree(self.directory_path)
        self.assertFalse(os.path.isdir(self.directory_path))
        
        for row in PdsArchiveRecord.query.all():
            db_session.delete(row)
        db_session.commit()
    
    def test_for_smoke(self):
        count = PdsArchiveRecord.query.count()
        self.assertTrue(count is not None)
        
    def test_insert(self):
        '''Test that string formats are handled'''
        before_count = PdsArchiveRecord.query.count()
        date_now = time_utilities.utc_now().replace(tzinfo=None)   # sqlite can't store timezone information
        pa = PdsArchiveRecord(date_now,
                              date_now,
                              date_now,
                              'command line',
                              'configuration',
                              False,
                              '/path/to/nowhere/',
                              'bundle_file_name',
                              'manifest_file_name',
                              'checksum_file_name',
                              1,
                              'generation_result',
                              'pds_status',
                              'notes')
        db_session.add(pa)
        db_session.commit()
        after_count = PdsArchiveRecord.query.count()
        self.assertEqual(after_count, before_count + 1)
        
        pds = PdsArchiveRecord.query.first()
        self.assertEqual(pds.generation_time, date_now)
        self.assertEqual(pds.start_time, date_now)
        self.assertEqual(pds.end_time,date_now)
        self.assertEqual(pds.command_line, 'command line')
        self.assertEqual(pds.configuration, 'configuration')
        self.assertFalse(pds.dry_run)
        self.assertEqual(pds.result_directory, '/path/to/nowhere/')
        self.assertEqual(pds.bundle_file_name, 'bundle_file_name')
        self.assertEqual(pds.manifest_file_name, 'manifest_file_name')
        self.assertEqual(pds.checksum_file_name, 'checksum_file_name')
        self.assertEqual(pds.result_version, 1)
        self.assertEqual(pds.generation_result, 'generation_result')
        self.assertEqual(pds.pds_status, 'pds_status')
        self.assertEqual(pds.notes, 'notes')
        
        string_rep = PdsArchiveRecord(pds.generation_time,
                                      pds.start_time,
                                      pds.end_time,
                                      pds.command_line,
                                      pds.configuration,
                                      pds.dry_run,
                                      pds.result_directory,
                                      pds.bundle_file_name,
                                      pds.manifest_file_name,
                                      pds.checksum_file_name,
                                      pds.result_version,
                                      pds.generation_result,
                                      pds.pds_status,
                                      pds.notes)
        comparison = '''
Generation Time:%s
Archive Start:%s
Archive End:%s
Command Line: %s
Configuration: %s
Dry Run: %s
Results Directory: %s
Bundle file:%s
Manifest file:%s
Checksum file:%s
Result Version:%s
Generation Result:%s
PDS Status:%s
Notes:%s
''' % (pds.generation_time,
            pds.start_time,
            pds.end_time,
            pds.command_line,
            pds.configuration,
            pds.dry_run,
            pds.result_directory,
            pds.bundle_file_name,
            pds.manifest_file_name,
            pds.checksum_file_name,
            pds.result_version,
            pds.generation_result,
            pds.pds_status,
            pds.notes)
        self.assertEqual(str(string_rep), comparison)