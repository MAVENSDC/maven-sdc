import os
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
import unittest
from maven_data_file_indexer import maven_file_indexer, utilities as indexer_utilities
from shutil import rmtree
from maven_database.models import ScienceFilesMetadata
from tests.maven_test_utilities.file_system import get_temp_root_dir
from tests.maven_test_utilities.db_utils import delete_data


class InsertScienceFilesWhenThereAreL0FilesTestCase(unittest.TestCase):
    '''Tests inserts of science files metadata when there are level 0 files in the directory tree.'''

    def setUp(self):
        self.root_directory = get_temp_root_dir(create=False)
        self.directory_path = os.path.join(self.root_directory, 'path/to/nowhere')
        if not os.path.isdir(self.directory_path):
            os.makedirs(self.directory_path)
        l0b_filename = 'mvn_ins_grp_l0b_20130430_v01.dat'
        self.l0b_filename = os.path.join(self.directory_path, l0b_filename)
        self.file_size = 1234
        f = open(self.l0b_filename, 'w')
        f.write('l0b' * self.file_size)
        f.close()
        self.l2_filename = 'mvn_lpw_l2_lpiv_20140514_v01_r01.cdf'
        self.l2_filename = os.path.join(self.directory_path, self.l2_filename)
        self.file_size = 12345
        with open(self.l2_filename, 'w') as f:
            f.write('l2' * self.file_size)

    def tearDown(self):
        rmtree(self.root_directory)
        self.assertFalse(os.path.isdir(self.root_directory))
        delete_data(ScienceFilesMetadata)

    def test_attempt_to_insert_duplicate_l0_metadata(self):
        self.assertTrue(os.path.isfile(self.l0b_filename))
        l0b_metadata = indexer_utilities.get_metadata_for_l0_file(self.l0b_filename)
        self.assertTrue(l0b_metadata)
        indexer_utilities.insert_l0_file_metadata([l0b_metadata])
        self.assertEqual(ScienceFilesMetadata.query.filter(ScienceFilesMetadata.level.startswith('l0')).count(), 1)  # the level 0 metadata are in the database

        self.assertEqual(ScienceFilesMetadata.query.filter(ScienceFilesMetadata.level.startswith('l2')).count(), 0)  # there are no level 2 metadata in the database
        maven_file_indexer.run_full_index([self.root_directory])
        self.assertEqual(ScienceFilesMetadata.query.filter(ScienceFilesMetadata.level.startswith('l2')).count(), 1)  # the level 2 metadata made it into the database

        l2_metadata = ScienceFilesMetadata.query.filter(ScienceFilesMetadata.level.startswith('l2')).first()
        self.assertEqual(l2_metadata.directory_path, self.directory_path)
        self.assertEqual(l2_metadata.file_name, os.path.basename(self.l2_filename))
        self.assertEqual(l2_metadata.file_size, os.path.getsize(self.l2_filename))
