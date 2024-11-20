'''
Created on Jun 8, 2016

@author: bstaley
'''
import unittest
import os
from shutil import rmtree
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'

from maven_database.models import ScienceFilesMetadata, AncillaryFilesMetadata
from maven_public import utilities as maven_public_utils
from tests.maven_test_utilities import file_system, db_utils


class TestPublicReleased(unittest.TestCase):

    def setUp(self):
        self.test_root = file_system.get_temp_root_dir()
        self.site_root = os.path.join(self.test_root, 'site')
        os.mkdir(self.site_root)
        self.data_root = os.path.join(self.test_root, 'data')
        self.sci_data_root = os.path.join(self.data_root, 'sci')
        self.anc_data_root = os.path.join(self.data_root, 'anc')

        self.sci_files = ['mvn_test_l1_20090516_v%02d_r%02d.txt' % divmod(i, 10) for i in range(100)]
        self.anc_files = ['sci_anc_l1_20090516_v%04d.txt' % i for i in range(100)]
        self.anc_in_sci_files = ['sci_anc_l2_20090516_v%04d.txt' % i for i in range(10)]
        self.sci_in_anc_files = ['mvn_test_l2_20090516_v%02d_r%02d.txt' % divmod(i, 10) for i in range(10)]

        # Build test files
        file_system.build_test_files_and_structure(default_file_contents='something to fill the file',
                                                   files_base_dir=self.sci_data_root,
                                                   files_list=self.sci_files)
        file_system.build_test_files_and_structure(default_file_contents='something to fill the file',
                                                   files_base_dir=self.anc_data_root,
                                                   files_list=self.sci_in_anc_files)
        file_system.build_test_files_and_structure(default_file_contents='something to fill the file',
                                                   files_base_dir=self.anc_data_root,
                                                   files_list=self.anc_files)
        file_system.build_test_files_and_structure(default_file_contents='something to fill the file',
                                                   files_base_dir=self.sci_data_root,
                                                   files_list=self.anc_in_sci_files)

        # Add science files to metadata
        for _next in self.sci_files:
            db_utils.insert_science_files_metadata(file_name=_next,
                                                   released=True)
        for _next in self.anc_files:
            db_utils.insert_ancillary_file_metadata(file_name=_next,
                                                    released=True)

        for _next in self.anc_in_sci_files:
            db_utils.insert_ancillary_file_metadata(file_name=_next,
                                                    released=True)

        for _next in self.sci_in_anc_files:
            db_utils.insert_ancillary_file_metadata(file_name=_next,
                                                    released=True)

        # create a release file with the data generated above
        self.test_root = file_system.get_temp_root_dir()
        self.data_root = os.path.join(self.test_root, 'data')
        os.mkdir(self.data_root)
        self.released_list_file = self.data_root + "/temp-released-site-list-2022-11-07 19:32:46.341471.txt"
        sci_anc_files = self.sci_files + self.anc_files + self.anc_in_sci_files + self.sci_in_anc_files
        with open(self.released_list_file, "a") as f:
            for file in sci_anc_files:
                f.write(file + "\n")

    def tearDown(self):
        rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))
        db_utils.delete_data(ScienceFilesMetadata, AncillaryFilesMetadata)

    def testClearReleased(self):
        maven_public_utils.clear_released()
        db_results = ScienceFilesMetadata.query.filter(ScienceFilesMetadata.released).all()
        self.assertTrue(len(db_results) == 0)
        db_results = AncillaryFilesMetadata.query.filter(AncillaryFilesMetadata.released).all()
        self.assertTrue(len(db_results) == 0)

    def testUpdateReleased(self):
        # Ensure nothing is released
        maven_public_utils.clear_released()

        maven_public_utils.update_released(self.released_list_file)
        db_results = ScienceFilesMetadata.query.filter(ScienceFilesMetadata.released == False).all()
        self.assertTrue(len(db_results) == 0)
        db_results = AncillaryFilesMetadata.query.filter(AncillaryFilesMetadata.released == False).all()
        self.assertTrue(len(db_results) == 0)


class TestMetadataGenerator():

    def __init__(self, list_of_return_data):
        self.data = list_of_return_data

    def generate(self):
        for _next in self.data:
            yield _next
