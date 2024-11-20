import os
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
import unittest
import datetime
from maven_data_file_indexer import utilities as indexer_utilities
from shutil import rmtree
from tests.maven_test_utilities.file_system import get_temp_root_dir


class EUVFlareMetadataTestCase(unittest.TestCase):
    '''Tests computation of metadata for EUV Flare files.'''

    def setUp(self):
        self.root_directory = get_temp_root_dir(create=False)
        self.directory_path = os.path.join(self.root_directory, 'path/to/nowhere')
        if not os.path.isdir(self.directory_path):
            os.makedirs(self.directory_path)
        flare_filename = "mvn_euv_flare_20230506_1121_M1.8.png"
        flare_catalog_filename = "mvn_euv_flare_catalog_2023.txt"
        euv_filename = "mvn_euv_20230623_1112.png"
        self.flare_filename = os.path.join(self.directory_path, flare_filename)
        self.flare_catalog_filename = os.path.join(self.directory_path, flare_catalog_filename)
        self.euv_filename = os.path.join(self.directory_path, euv_filename)
        self.file_size = 1234

        for filename in [self.flare_catalog_filename, self.flare_filename, self.euv_filename]:
            with open(filename, 'w') as f:
                f.write('a' * self.file_size)

    def tearDown(self):
        rmtree(self.root_directory)
        self.assertFalse(os.path.isdir(self.root_directory))

    def test_euv_flare_good_filename(self):
        metadata = indexer_utilities.get_metadata_for_science_file(self.flare_filename)
        self.assertTrue(metadata)
        self.assertEqual(metadata.directory_path, self.directory_path)
        self.assertEqual(metadata.file_name, os.path.basename(self.flare_filename))
        self.assertEqual(metadata.file_size, os.path.getsize(self.flare_filename))
        self.assertEqual(metadata.instrument, 'euv')
        self.assertEqual(metadata.flare_class, 'M1.8')
        timetag = datetime.datetime(2023, 5, 6, 11, 21, 0)
        timetag_utc = time_utilities.to_utc_tz(timetag)
        self.assertEqual(metadata.timetag, timetag_utc)

    def test_euv_flare_catalog_good_filename(self):
        metadata = indexer_utilities.get_metadata_for_science_file(self.flare_catalog_filename)
        self.assertTrue(metadata)
        self.assertEqual(metadata.directory_path, self.directory_path)
        self.assertEqual(metadata.file_name, os.path.basename(self.flare_catalog_filename))
        self.assertEqual(metadata.file_size, os.path.getsize(self.flare_catalog_filename))
        self.assertEqual(metadata.instrument, 'euv')
        timetag = datetime.datetime(2023, 1, 1, 0, 0, 0)
        timetag_utc = time_utilities.to_utc_tz(timetag)
        self.assertEqual(metadata.timetag, timetag_utc)

    def test_euv_good_filename(self):
        metadata = indexer_utilities.get_metadata_for_science_file(self.euv_filename)
        self.assertTrue(metadata)
        self.assertEqual(metadata.directory_path, self.directory_path)
        self.assertEqual(metadata.file_name, os.path.basename(self.euv_filename))
        self.assertEqual(metadata.file_size, os.path.getsize(self.euv_filename))
        self.assertEqual(metadata.instrument, 'euv')
        timetag = datetime.datetime(2023, 6, 23, 11, 12, 0)
        timetag_utc = time_utilities.to_utc_tz(timetag)
        self.assertEqual(metadata.timetag, timetag_utc)