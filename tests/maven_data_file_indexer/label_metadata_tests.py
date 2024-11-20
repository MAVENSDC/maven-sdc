import os
import unittest
from shutil import rmtree
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from maven_data_file_indexer import utilities as indexer_utilities
from tests.maven_test_utilities import file_system
from maven_utilities import maven_config


class LabelMetadataTests(unittest.TestCase):
    '''Tests computation of metadata for label files.'''

    def setUp(self):
        self.root_directory = file_system.get_temp_root_dir()

        # (expected version,expected revision,'file name')
        self.test_label_files = [
            (2, 2, 'mvn_ngi_l2_ion-abund-14630_20150113t215140_v02_r02.xml'),  # version/revision provided
            (1, 0, 'mvn_iuv_l1a_apoapse-orbit00621-muv_20150124t165026.xml'),  # no version/revision
            (0, 4, 'mvn_sta_l2_d8-12r1e_20140401_v00_r04.cdf'),
            (6, 2, 'mvn_ngi_l1b_osnb-14202_20141204T014042_v06_r02.xml.gz'),
            (1, 1, 'mvn_mag_l2_2015028pc_20150128_v01_r01.sts')
        ]
        file_system.build_test_files_and_structure('some bogus content to get a file size',
                                                   self.root_directory,
                                                   [f for _, _, f in self.test_label_files])

    def tearDown(self):
        rmtree(self.root_directory)
        self.assertFalse(os.path.isdir(self.root_directory))

    def test_filename_regex(self):
        m = maven_config.label_regex.match('mvn_ngi_l2_ion-abund-14630_20150113t215140_v02_r02.xml')  # Contains version/revision
        self.assertIsNotNone(m)
        m = maven_config.label_regex.match('mvn_iuv_l1a_apoapse-orbit00621-muv_20150124T165026.xml')  # No version/revision
        self.assertIsNotNone(m)
        m = maven_config.label_regex.match('mvn_iuv_l1a_apoapse-orbit00621-muv_20150124t165026.xml')  # Little t in YYYYMMDDtHHMMSS
        self.assertIsNotNone(m)
        m = maven_config.label_regex.match('mvn_iuv_l1a_apoapse-orbit00621-muv_20150124t165026.xml.gz')  # gz extension
        self.assertIsNotNone(m)

    def test_label_metadata_generation(self):
        for expected_ver, expected_rev, fully_qualified_test_file in [(v, r, os.path.join(self.root_directory, f)) for v, r, f in self.test_label_files]:
            metadata = indexer_utilities.get_metadata_for_science_file(fully_qualified_test_file)
            self.assertTrue(metadata)
            self.assertEqual(expected_ver, metadata.version)
            self.assertEqual(expected_rev, metadata.revision)

    def test_orbit_metadata_generation(self):
        expected_orbits = [None, '00621', None, None, None]
        fully_qualified_test_file = [os.path.join(self.root_directory, f) for _, _, f in self.test_label_files]
        for exp_orbit, test_file in zip(expected_orbits, fully_qualified_test_file):
            metadata = indexer_utilities.get_metadata_for_science_file(test_file)
            self.assertEqual(exp_orbit, metadata.orbit)
