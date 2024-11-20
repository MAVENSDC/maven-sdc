import os
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
import unittest
import datetime
from maven_data_file_indexer import utilities as indexer_utilities
from shutil import rmtree
from tests.maven_test_utilities.file_system import get_temp_root_dir
from maven_utilities import maven_config


class QlMetadataTestCase(unittest.TestCase):
    '''Tests computation of metadata for quicklook files.'''

    def setUp(self):
        self.root_directory = get_temp_root_dir(create=False)
        self.directory_path = os.path.join(self.root_directory, 'path/to/nowhere')
        if not os.path.isdir(self.directory_path):
            os.makedirs(self.directory_path)
        ql_filename = 'mvn_ins_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf'
        self.filename = os.path.join(self.directory_path, ql_filename)
        self.file_size = 1234
        with open(self.filename, 'w') as f:
            f.write('a' * self.file_size)

    def tearDown(self):
        rmtree(self.root_directory)
        self.assertFalse(os.path.isdir(self.root_directory))

    def test_ql_filename_regex(self):
        m = maven_config.science_regex.match('mvn_ins_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.png')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.fits')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.txt')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.csv')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_ql_testplan-testorbit-testmode-testdatatype_20130430_v01_r02.txt')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_ql_descriptor-with-hyphen_20130430_v01_r02.txt')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf.gz')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.png.gz')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.fits.gz')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.txt.gz')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.csv.gz')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_ql_testplan-testorbit-testmode-testdatatype_20130430_v01_r02.txt.gz')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_iuv_ql_o3-orbit00350-muv_20141203T224805_v04_r01.jpg')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_ql_descriptor-with-hyphen_20130430_v01_r02.txt.gz')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('something_completely_bogus')
        self.assertTrue(m is None)
        m = maven_config.science_regex.match('mvn_ins_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_v01.cdf')  # missing revision
        self.assertTrue(m is None)
        m = maven_config.science_regex.match('mvn_ins_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_r02.cdf')  # missing version
        self.assertTrue(m is None)
        m = maven_config.science_regex.match('mvn_ins_ql_testplan-testorbit-testmode-testdatatype_v01_r02.cdf')  # missing timetag
        self.assertTrue(m is None)
        m = maven_config.science_regex.match('mvn_ins_ql_20130430T010203_v01_r02.cdf')  # missing descriptor
        self.assertTrue(m is not None)  # descriptors are optional
        m = maven_config.science_regex.match('mvn_ins_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf')  # missing level
        self.assertTrue(m is None)
        m = maven_config.science_regex.match('mvn_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf')  # missing instrument
        self.assertTrue(m is None)
        m = maven_config.science_regex.match('not_mvn_ins_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf')  # starts with something other than "mvn"
        self.assertTrue(m is None)

    def test_ql_metadata_for_good_filename(self):
        metadata = indexer_utilities.get_metadata_for_science_file(self.filename)
        self.assertTrue(metadata)
        self.assertEqual(metadata.directory_path, self.directory_path)
        self.assertEqual(metadata.file_name, os.path.basename(self.filename))
        self.assertEqual(metadata.file_size, os.path.getsize(self.filename))
        self.assertEqual(metadata.instrument, 'ins')
        self.assertEqual(metadata.level, 'ql')
        self.assertEqual(metadata.descriptor, 'testplan-testorbit-testmode-testdatatype')
        timetag = datetime.datetime(2013, 4, 30, 1, 2, 3)
        timetag_utc = time_utilities.to_utc_tz(timetag)
        self.assertEqual(metadata.timetag, timetag_utc)
        self.assertEqual(metadata.version, 1)
        self.assertEqual(metadata.revision, 2)

    def test_ql_metadata_for_bad_filename(self):
        filename = 'ins_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf'  # does not start with mvn
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf'  # has no instrument
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf'  # has no level
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_ql_testplan-testorbit-testmode-testdatatype_0430T010203_v01_r02.cdf'  # has no year
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_ql_testplan-testorbit-testmode-testdatatype_201330T010203_v01_r02.cdf'  # has no month
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_ql_testplan-testorbit-testmode-testdatatype_201304T010203_v01_r02.cdf'  # has no day
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_ql_testplan-testorbit-testmode-testdatatype_20130430T010203_r02.cdf'  # has no version
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
