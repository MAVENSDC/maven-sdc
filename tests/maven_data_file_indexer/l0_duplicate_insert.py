import os
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
import unittest
from maven_data_file_indexer import maven_file_indexer, utilities as indexer_utilities
from shutil import rmtree
from maven_database.models import ScienceFilesMetadata
from tests.maven_test_utilities.file_system import get_temp_root_dir
from tests.maven_test_utilities.db_utils import delete_data


class L0DuplicateInsertTestCase(unittest.TestCase):
    '''Tests attempt to insert duplicate metadata level 0 file.'''

    def setUp(self):
        self.root_directory = get_temp_root_dir(create=False)
        self.directory_path = os.path.join(self.root_directory, 'path/to/nowhere')
        if not os.path.isdir(self.directory_path):
            os.makedirs(self.directory_path)
        l0b_filename = 'mvn_ins_grp_l0b_20130430_v01.dat'
        self.filename = os.path.join(self.directory_path, l0b_filename)
        self.file_size = 1234
        with open(self.filename, 'w') as f:
            f.write('a' * self.file_size)

        delete_data(ScienceFilesMetadata)

    def tearDown(self):
        rmtree(self.root_directory)
        self.assertFalse(os.path.isdir(self.root_directory))
        delete_data(ScienceFilesMetadata)

    def test_attempt_to_insert_duplicate_l0_metadata(self):
        metadata = list(indexer_utilities.generate_metadata_for_l0_file([self.filename]))
        indexer_utilities.insert_l0_file_metadata(metadata)
        self.assertEqual(ScienceFilesMetadata.query.filter(ScienceFilesMetadata.level.startswith('l0')).count(), 1)
        l0_metadata = ScienceFilesMetadata.query.filter(ScienceFilesMetadata.level.startswith('l0')).first()

        # Do the usual traversal of the directory tree and try to reinsert the file.
        maven_file_indexer.run_full_index([self.root_directory])

        # There should be no difference in the metadata that are in the database.
        self.assertEqual(ScienceFilesMetadata.query.filter(ScienceFilesMetadata.level.startswith('l0')).count(), 1)
        post_metadata = ScienceFilesMetadata.query.filter(ScienceFilesMetadata.level.startswith('l0')).first()

        self.assertEqual(l0_metadata.directory_path, post_metadata.directory_path)
        self.assertEqual(l0_metadata.file_name, post_metadata.file_name)
        self.assertEqual(l0_metadata.file_size, post_metadata.file_size)
