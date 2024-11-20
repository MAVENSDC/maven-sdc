import sys
sys.path.append('../../maven_data_file_indexer')
import os
import unittest
import datetime
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
from maven_data_file_indexer import utilities as indexer_utilities
from shutil import rmtree
from tests.maven_test_utilities.file_system import get_temp_root_dir
from maven_utilities import maven_config


class KpMetadataTestCase(unittest.TestCase):
    '''Tests computation of metadata for key parameter files.'''

    def setUp(self):
        self.root_directory = get_temp_root_dir(create=False)
        self.directory_path = os.path.join(self.root_directory, 'path/to/nowhere')
        if not os.path.isdir(self.directory_path):
            os.makedirs(self.directory_path)
        assert os.path.isdir(self.directory_path)
        kp_filename = 'mvn_kp_insitu_20130430T010203_v01_r02.tab'
        self.filename = os.path.join(self.directory_path, kp_filename)
        self.file_size = 1234
        with open(self.filename, 'w') as f:
            f.write('a' * self.file_size)

    def tearDown(self):
        rmtree(self.root_directory)
        self.assertFalse(os.path.isdir(self.root_directory))

    def test_kp_filename_regex(self):
        m = maven_config.kp_regex.match('mvn_kp_insitu_20130430T010203_v01_r02.tab')
        self.assertTrue(m is not None)
        m = maven_config.kp_regex.match('mvn_kp_insitu_20130430T010203_v01_r02.tab.gz')
        self.assertTrue(m is not None)
        m = maven_config.kp_regex.match('mvn_kp_insitu_20130430_v01_r02.tab')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('something_completely_bogus')
        self.assertTrue(m is None)
        m = maven_config.kp_regex.match('mvn_kp_insitu_20130430_v01.tab')  # mission revision
        self.assertTrue(m is None)
        m = maven_config.kp_regex.match('mvn_kp_insitu_20130430_r02.tab')  # mission version
        self.assertTrue(m is None)
        m = maven_config.kp_regex.match('mvn_kp_insitu_v01_r02.tab')  # missing timetag
        self.assertTrue(m is None)
        m = maven_config.kp_regex.match('mvn_kp_insitu_descriptor_20130430T010203_v01_r02.tab')  # TODO check the regex unexpected descriptor
        self.assertTrue(m is not None)
        m = maven_config.kp_regex.match('mvn_kp_20130430T010203_v01_r02.tab')  # missing level
        self.assertTrue(m is None)
        m = maven_config.kp_regex.match('mvn_insitu_20130430T010203_v01_r02.tab')  # missing instrument
        self.assertTrue(m is None)
        m = maven_config.kp_regex.match('maven_kp_insitu_20130430T010203_v01_r02.tab')  # starts with wrong mission
        self.assertTrue(m is None)

    def test_kp_metadata_for_good_filename(self):
        metadata = list(indexer_utilities.generate_metadata_for_science_file([self.filename]))
        self.assertEqual(1, len(metadata))
        self.assertEqual(metadata[0].directory_path, self.directory_path)
        self.assertEqual(metadata[0].file_name, os.path.basename(self.filename))
        self.assertEqual(metadata[0].file_size, os.path.getsize(self.filename))
        self.assertEqual(metadata[0].instrument, 'kp')
        self.assertEqual(metadata[0].level, 'insitu')
        self.assertEqual(metadata[0].descriptor, '')
        timetag = datetime.datetime(2013, 4, 30, 1, 2, 3)
        timetag_utc = time_utilities.to_utc_tz(timetag)
        self.assertEqual(metadata[0].timetag, timetag_utc)
        self.assertEqual(metadata[0].version, 1)
        self.assertEqual(metadata[0].revision, 2)

    def test_kp_metadata_for_bad_filename(self):
        filename = 'maven_kp_insitu_20130430T010203_v01_r02.tab'  # does not start with mvn
        metadata = list(indexer_utilities.generate_metadata_for_science_file([filename]))
        self.assertEqual(0, len(metadata))
        filename = 'mvn_insitu_20130430T010203_v01_r02.tab'  # has no instrument
        metadata = list(indexer_utilities.generate_metadata_for_science_file([filename]))
        self.assertEqual(0, len(metadata))
        filename = 'mvn_kp_20130430T010203_v01_r02.tab'  # has no level
        metadata = list(indexer_utilities.generate_metadata_for_science_file([filename]))
        self.assertEqual(0, len(metadata))
        filename = 'mvn_kp_insitu_0430T010203_v01_r02.tab'  # has no year
        metadata = list(indexer_utilities.generate_metadata_for_science_file([filename]))
        self.assertEqual(0, len(metadata))
        filename = 'mvn_kp_insitu_201330T010203_v01_r02.tab'  # has no month
        metadata = list(indexer_utilities.generate_metadata_for_science_file([filename]))
        self.assertEqual(0, len(metadata))
        filename = 'mvn_kp_insitu_201304T010203_v01_r02.tab'  # has no day
        metadata = list(indexer_utilities.generate_metadata_for_science_file([filename]))
        self.assertEqual(0, len(metadata))
        filename = 'mvn_kp_insitu_20130430_r02.tab'  # has no version
        metadata = list(indexer_utilities.generate_metadata_for_science_file([filename]))
        self.assertEqual(0, len(metadata))
