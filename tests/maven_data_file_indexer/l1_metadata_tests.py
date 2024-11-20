import os
import unittest
import datetime
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
from maven_data_file_indexer import utilities as indexer_utilities
from shutil import rmtree
from tests.maven_test_utilities.file_system import get_temp_root_dir
from maven_utilities import maven_config


class L1MetadataTestCase(unittest.TestCase):
    '''Tests computation of metadata for level 1 files.'''

    def setUp(self):
        self.root_directory = get_temp_root_dir(create=False)
        self.directory_path = os.path.join(self.root_directory, 'path/to/nowhere')
        if not os.path.isdir(self.directory_path):
            os.makedirs(self.directory_path)
        l1_filename = 'mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf'
        self.filename = os.path.join(self.directory_path, l1_filename)
        self.file_size = 1234
        with open(self.filename, 'w') as f:
            f.write('a' * self.file_size)

    def tearDown(self):
        rmtree(self.root_directory)
        self.assertFalse(os.path.isdir(self.root_directory))

    def test_l1_filename_regex(self):
        m = maven_config.science_regex.match('mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.png')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.fits')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.txt')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.csv')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf.gz')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.png.gz')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.fits.gz')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.txt.gz')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.csv.gz')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_l1a_descriptor-with-hyphen_20130430T010203_v01_r02.cdf')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430_v01_r02.cdf')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_iuv_l1a_muv-ISON1-narrowshort-cycle06_20131213T081639_v00_r00.fits.gz')
        self.assertTrue(m is not None)
        m = maven_config.science_regex.match('mvn_ins_l1a_20130430T010203_v01_r02.cdf')  # missing descriptor
        self.assertTrue(m is not None)  # descriptors are optional
        m = maven_config.science_regex.match('something_completely_bogus')
        self.assertTrue(m is None)
        m = maven_config.science_regex.match('mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01.cdf')  # missing revision
        self.assertTrue(m is None)
        m = maven_config.science_regex.match('mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_r02.cdf')  # missing version
        self.assertTrue(m is None)
        m = maven_config.science_regex.match('mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_v01_r02.cdf')  # missing timetag
        self.assertTrue(m is None)
        m = maven_config.science_regex.match('mvn_ins_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf')  # missing level
        self.assertTrue(m is None)
        m = maven_config.science_regex.match('mvn_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf')  # missing instrument
        self.assertTrue(m is None)
        m = maven_config.science_regex.match('not_mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf')  # starts with something other than "mvn"
        self.assertTrue(m is None)

    def test_l1_metadata_for_good_filename(self):
        metadata = indexer_utilities.get_metadata_for_science_file(self.filename)
        self.assertTrue(metadata)
        self.assertEqual(metadata.directory_path, self.directory_path)
        self.assertEqual(metadata.file_name, os.path.basename(self.filename))
        self.assertEqual(metadata.file_size, os.path.getsize(self.filename))
        self.assertEqual(metadata.instrument, 'ins')
        self.assertEqual(metadata.level, 'l1a')
        self.assertEqual(metadata.descriptor, 'testplan-testorbit-testmode-testdatatype')
        timetag = datetime.datetime(2013, 4, 30, 1, 2, 3)
        timetag_utc = time_utilities.to_utc_tz(timetag)
        self.assertEqual(metadata.timetag, timetag_utc)
        self.assertEqual(metadata.version, 1)
        self.assertEqual(metadata.revision, 2)

    def test_l1_metadata_for_bad_filename(self):
        filename = 'ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf'  # does not start with mvn
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf'  # has no instrument
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf'  # has no level
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_0430T010203_v01_r02.cdf'  # has no year
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_201330T010203_v01_r02.cdf'  # has no month
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_201304T010203_v01_r02.cdf'  # has no day
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430010203_v01_r02.cdf'  # has no T
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T0203_v01_r02.cdf'  # has no hour
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T0103_v01_r02.cdf'  # has no minute
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T0102_v01_r02.cdf'  # has no second
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_r02.cdf'  # has no version
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)
        filename = 'mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01.cdf'  # has no revision
        metadata = indexer_utilities.get_metadata_for_science_file(filename)
        self.assertFalse(metadata)

    def test_extraction_of_l1_file_extension(self):
        l1_filename = 'mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf'
        file_path = os.path.join(self.directory_path, l1_filename)
        file_size = 1234
        with open(file_path, 'w') as f:
            f.write('a' * file_size)
        metadata = indexer_utilities.get_metadata_for_science_file(file_path)
        self.assertEqual(metadata.file_extension, 'cdf')

        l1_filename = 'mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf.gz'
        file_path = os.path.join(self.directory_path, l1_filename)
        file_size = 1234
        with open(file_path, 'w') as f:
            f.write('a' * file_size)
        metadata = indexer_utilities.get_metadata_for_science_file(file_path)
        self.assertEqual(metadata.file_extension, 'cdf')

    def test_extraction_of_l1_file_plan(self):
        l1_filename = 'mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf'
        file_path = os.path.join(self.directory_path, l1_filename)
        file_size = 1234
        with open(file_path, 'w') as f:
            f.write('a' * file_size)
        metadata = indexer_utilities.get_metadata_for_science_file(file_path)
        self.assertEqual(metadata.plan, 'testplan')

    def test_extraction_of_l1_file_orbit(self):
        l1_filename = 'mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf'
        file_path = os.path.join(self.directory_path, l1_filename)
        file_size = 1234
        with open(file_path, 'w') as f:
            f.write('a' * file_size)
        metadata = indexer_utilities.get_metadata_for_science_file(file_path)
        self.assertEqual(metadata.orbit, None)

    def test_extraction_of_l1_file_mode(self):
        l1_filename = 'mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf'
        file_path = os.path.join(self.directory_path, l1_filename)
        file_size = 1234
        with open(file_path, 'w') as f:
            f.write('a' * file_size)
        metadata = indexer_utilities.get_metadata_for_science_file(file_path)
        self.assertEqual(metadata.mode, 'testmode')

    def test_extraction_of_l1_file_data_type(self):
        l1_filename = 'mvn_ins_l1a_testplan-testorbit-testmode-testdatatype_20130430T010203_v01_r02.cdf'
        file_path = os.path.join(self.directory_path, l1_filename)
        file_size = 1234
        with open(file_path, 'w') as f:
            f.write('a' * file_size)
        metadata = indexer_utilities.get_metadata_for_science_file(file_path)
        self.assertEqual(metadata.data_type, 'testdatatype')
