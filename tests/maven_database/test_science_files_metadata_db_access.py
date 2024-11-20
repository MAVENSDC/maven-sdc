import os
import unittest
import datetime
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from maven_database.models import ScienceFilesMetadata
from shutil import rmtree
from tests.maven_test_utilities import file_system
from tests.maven_test_utilities.db_utils import delete_data, insert_science_files_metadata


class ScienceFilesMetadataDbAccessTestCase(unittest.TestCase):
    '''Tests access of the database table that holds the science
    files metadata.
    '''

    def setUp(self):
        self.timetag = datetime.datetime(2013, 4, 22, 0, 0, 0)
        self.directory_path = file_system.get_temp_root_dir()

    def tearDown(self):
        rmtree(self.directory_path)
        self.assertFalse(os.path.isdir(self.directory_path))
        delete_data(ScienceFilesMetadata)

    def test_for_smoke(self):
        count = ScienceFilesMetadata.query.count()
        self.assertTrue(count is not None)

    def test_no_mod_date(self):
        '''Test that mod_date none defaults to now()'''
        t = datetime.datetime.now()
        now = datetime.datetime(t.year, t.month, t.day,
                                t.hour, t.minute, t.second)
        _ = insert_science_files_metadata('test_file.txt',
                                          'test_file.txt',
                                          '/path/to/nowhere',
                                          0,
                                          'test_instr',
                                          'test_level',
                                          now,
                                          0,
                                          - 1,
                                          grouping='test_grouping',
                                          descriptor='test_descriptor',
                                          revision=-2,
                                          mod_date=None,
                                          extension='txt',
                                          plan='testplan',
                                          orbit='testorbit',
                                          mode='testmode',
                                          data_type='testdatatype',
                                          released=False)
        m2 = ScienceFilesMetadata.query.first()
        # defaults to utc.now()
        self.assertIsNotNone(m2.mod_date.month)
        self.assertIsNotNone(m2.mod_date.day)
        self.assertIsNotNone(m2.mod_date.year)

    def test_insert(self):
        '''Test that string formats are handled'''
        before_count = ScienceFilesMetadata.query.count()
        t = datetime.datetime.now()
        mtime = datetime.datetime.now()
        now = datetime.datetime(t.year, t.month, t.day,
                                t.hour, t.minute, t.second)
        _ = insert_science_files_metadata('test_file.txt',
                                          'test_file.txt',
                                          '/path/to/nowhere',
                                          0,
                                          'test_instr',
                                          'test_level',
                                          now,
                                          0,
                                          - 1,
                                          grouping='test_grouping',
                                          descriptor='test_descriptor',
                                          revision=-2,
                                          mod_date=mtime,
                                          extension='txt',
                                          plan='testplan',
                                          orbit='testorbit',
                                          mode='testmode',
                                          data_type='testdatatype',
                                          released=False)

        after_count = ScienceFilesMetadata.query.count()
        self.assertEqual(after_count - 1, before_count)
        m2 = ScienceFilesMetadata.query.first()
        self.assertEqual(m2.file_name, 'test_file.txt')
        self.assertEqual(m2.file_root, 'test_file.txt')
        self.assertEqual(m2.directory_path, '/path/to/nowhere')
        self.assertEqual(m2.file_size, 0)
        self.assertEqual(m2.instrument, 'test_instr')
        self.assertEqual(m2.grouping, 'test_grouping')
        self.assertEqual(m2.level, 'test_level')
        self.assertEqual(m2.descriptor, 'test_descriptor')
        self.assertEqual(m2.timetag.year, now.year)
        self.assertEqual(m2.timetag.month, now.month)
        self.assertEqual(m2.timetag.day, now.day)
        self.assertEqual(m2.timetag.hour, now.hour)
        self.assertEqual(m2.timetag.minute, now.minute)
        self.assertEqual(m2.timetag.second, now.second)
        self.assertEqual(m2.absolute_version, 0)
        self.assertEqual(m2.version, -1)
        self.assertEqual(m2.revision, -2)
        self.assertEqual(m2.mod_date, mtime)
        self.assertEqual(m2.file_extension, 'txt')
        self.assertEqual(m2.plan, 'testplan')
        self.assertEqual(m2.orbit, 'testorbit')
        self.assertEqual(m2.mode, 'testmode')
        self.assertEqual(m2.data_type, 'testdatatype')
        self.assertEqual(m2.released, False)
        string_rep = ScienceFilesMetadata(m2.file_name,
                                          m2.file_root,
                                          m2.directory_path,
                                          m2.file_size,
                                          m2.instrument,
                                          m2.grouping,
                                          m2.level,
                                          m2.descriptor,
                                          m2.timetag,
                                          m2.version,
                                          m2.revision,
                                          m2.mod_date,
                                          m2.file_extension,
                                          m2.plan,
                                          m2.orbit,
                                          m2.mode,
                                          m2.data_type,
                                          m2.released)
        self.assertEqual(str(string_rep), "%s %s %d" % (m2.directory_path, m2.file_name, m2.file_size))
