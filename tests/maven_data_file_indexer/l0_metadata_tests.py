import os
import unittest
import datetime
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
from maven_data_file_indexer import utilities as indexer_utilities
from shutil import rmtree
from tests.maven_test_utilities.file_system import get_temp_root_dir
from maven_utilities import maven_config


class L0MetadataTestCase(unittest.TestCase):
    '''Tests computation of metadata for level 0 files.'''

    def setUp(self):
        self.root_directory = get_temp_root_dir(create=False)
        self.directory_path = os.path.join(self.root_directory, 'path/to/nowhere')
        if not os.path.isdir(self.directory_path):
            os.makedirs(self.directory_path)
        l0_filename = 'mvn_ins_grp_l0_20130430_v01.dat'
        self.filename = os.path.join(self.directory_path, l0_filename)
        self.file_size = 1234
        with open(self.filename, 'w') as f:
            f.write('a' * self.file_size)

    def tearDown(self):
        rmtree(self.root_directory)
        self.assertFalse(os.path.isdir(self.root_directory))

    def test_l0_file_regex(self):
        m = maven_config.l0_regex.match('mvn_ins_grp_l0_20130430_v01.dat')
        self.assertTrue(m is not None)
        m = maven_config.l0_regex.match('something_completely_bogus')
        self.assertTrue(m is None)
        m = maven_config.l0_regex.match('mvn_ins_grp_l0_v01.dat')  # missing date
        self.assertTrue(m is None)
        m = maven_config.l0_regex.match('mvn_ins_grp_20130430_v01.dat')  # missing level
        self.assertTrue(m is None)
        m = maven_config.l0_regex.match('mvn_ins_l0_20130430_v01.dat')  # missing grouping
        self.assertTrue(m is None)
        m = maven_config.l0_regex.match('mvn_grp_l0_20130430_v01.dat')  # missing instrument
        self.assertTrue(m is None)
        m = maven_config.l0_regex.match('not_mvn_ins_grp_l0_20130430_v01.dat')  # does not start with "mvn"
        self.assertTrue(m is None)

    def test_l0_metadata_for_good_filename(self):
        metadata = list(indexer_utilities.generate_metadata_for_l0_file([self.filename]))
        self.assertTrue(metadata)
        self.assertEqual(metadata[0].directory_path, self.directory_path)
        self.assertEqual(metadata[0].file_name, os.path.basename(self.filename))
        self.assertEqual(metadata[0].file_size, os.path.getsize(self.filename))
        self.assertEqual(metadata[0].instrument, 'ins')
        self.assertEqual(metadata[0].grouping, 'grp')
        self.assertEqual(metadata[0].level, 'l0')
        timetag = datetime.datetime(2013, 4, 30, 0, 0, 0)
        timetag_utc = time_utilities.to_utc_tz(timetag)
        self.assertEqual(metadata[0].timetag, timetag_utc)
        self.assertEqual(metadata[0].version, 1)

    def test_l0_metadata_for_bad_filename(self):
        filename = 'ins_grp_l0_20130430_v01.dat'  # does not start with mvn
        metadata = indexer_utilities.get_metadata_for_l0_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_grp_l0_20130430_v01.dat'  # does not have instrument
        metadata = indexer_utilities.get_metadata_for_l0_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_grp_20130430_v01.dat'  # does not have level
        metadata = indexer_utilities.get_metadata_for_l0_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_grp_l0_0430_v01.dat'  # does not have year
        metadata = indexer_utilities.get_metadata_for_l0_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_grp_l0_201330_v01.dat'  # does not have month
        metadata = indexer_utilities.get_metadata_for_l0_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_grp_l0_201304_v01.dat'  # does not have day
        metadata = indexer_utilities.get_metadata_for_l0_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_grp_l0_20130430_v.dat'  # does not have version
        metadata = indexer_utilities.get_metadata_for_l0_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_grp_l0_20130430_v1.dat'  # version is one digit
        metadata = indexer_utilities.get_metadata_for_l0_file(filename)
        self.assertFalse(metadata)
