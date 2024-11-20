'''
Unit tests for the MavenDbLogHandler
Created on Feb 25, 2017

@author: cosc3564
'''
import os
import unittest
from datetime import datetime
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from maven_database.models import AncillaryFilesMetadata
from shutil import rmtree
from tests.maven_test_utilities import file_system
from tests.maven_test_utilities.db_utils import delete_data, insert_ancillary_file_metadata


class AncillaryFilesMetadataDbAccessTestCase(unittest.TestCase):
    '''Tests access of the database table that holds the ancillary
    files metadata.
    '''
    
    def setUp(self):
        self.directory_path = file_system.get_temp_root_dir()
        
    def tearDown(self):
        rmtree(self.directory_path)
        self.assertFalse(os.path.isdir(self.directory_path))
        delete_data(AncillaryFilesMetadata)
        
    def test_for_smoke(self):
        count = AncillaryFilesMetadata.query.count()
        self.assertTrue(count is not None)

    def test_no_mod_date(self):
        '''Test that mod_date none defaults to now()'''
        _ = insert_ancillary_file_metadata('test_file.txt',
                                           'test_file.txt',
                                           '/path/to/nowhere',
                                           0,
                                           'product_test',
                                           'txt',
                                           None,
                                           datetime.strptime('2014-02-23 17:53:55.227619', '%Y-%m-%d %H:%M:%S.%f'),
                                           datetime.strptime('2014-02-23 17:53:55.227619', '%Y-%m-%d %H:%M:%S.%f'),
                                           1,
                                           released=False)

        m2 = AncillaryFilesMetadata.query.first()
        # defaults to utc.now()
        self.assertIsNotNone(m2.mod_date.month)
        self.assertIsNotNone(m2.mod_date.day)
        self.assertIsNotNone(m2.mod_date.year)
        
    def test_insert(self):
        '''Test that string formats are handled'''
        before_count = AncillaryFilesMetadata.query.count()
        _ = insert_ancillary_file_metadata('test_file.txt',
                                           'test_file.txt',
                                           '/path/to/nowhere',
                                           0,
                                           'product_test',
                                           'txt',
                                           datetime.strptime('2014-02-23 17:53:55.227619', '%Y-%m-%d %H:%M:%S.%f'),
                                           datetime.strptime('2014-02-23 17:53:55.227619', '%Y-%m-%d %H:%M:%S.%f'),
                                           datetime.strptime('2015-02-23 17:53:55.227619', '%Y-%m-%d %H:%M:%S.%f'),
                                           1,
                                           released=False)
        after_count = AncillaryFilesMetadata.query.count()
        self.assertEqual(after_count - 1 , before_count)
        m2 = AncillaryFilesMetadata.query.first()
        self.assertEqual(m2.file_name, 'test_file.txt')
        self.assertEqual(str(m2.directory_path), '/path/to/nowhere')
        self.assertEqual(m2.file_size, 0)
        string_rep = AncillaryFilesMetadata(m2.file_name,
                                            'file_root.txt',
                                            'test_file.txt',
                                            m2.directory_path,
                                            m2.file_size,
                                            'product_test',
                                            'ext',
                                            datetime.strptime('2014-02-23 17:53:55.227619', '%Y-%m-%d %H:%M:%S.%f'),
                                            datetime.strptime('2014-02-23 17:53:55.227619', '%Y-%m-%d %H:%M:%S.%f'),
                                            datetime.strptime('2015-02-23 17:53:55.227619', '%Y-%m-%d %H:%M:%S.%f'),
                                            released=False)
        self.assertEqual(str(string_rep), "%s %s %d" % (m2.directory_path, m2.file_name, m2.file_size))    
