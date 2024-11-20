'''
Created on Jan 20, 2016

@author: bstaley
'''
import unittest
import os
from shutil import rmtree

from maven_public import utilities as maven_public_utils
from tests.maven_test_utilities import file_system


class TestMavenPublicUtils(unittest.TestCase):

    def setUp(self):
        self.test_root = file_system.get_temp_root_dir()
        self.test_sym_root = os.path.join(self.test_root, 'symlinks')
        os.mkdir(self.test_sym_root)
        self.test_file_root = os.path.join(self.test_root, 'files')
        os.mkdir(self.test_file_root)

        self.test_files = ['test{0}'.format(i) for i in range(10)]  # some files

        # Build test files
        file_system.build_test_files_and_structure(default_file_contents='something to fill the file',
                                                   files_base_dir=self.test_file_root,
                                                   files_list=self.test_files)

        for next_file in self.test_files:
            os.symlink(os.path.join(self.test_file_root, next_file),
                       os.path.join(self.test_sym_root, next_file))

        self.removed_files = [_next[1] for _next in enumerate(self.test_files) if _next[0] % 2]

        for next_sym_file in self.removed_files:
            os.remove(os.path.join(self.test_file_root, next_sym_file))

        self.existing_files = [next_file for next_file in self.test_files if next_file not in self.removed_files]

    def tearDown(self):
        rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))

    def testScan(self):
        dead_links = [x for x in maven_public_utils.check_site(self.test_sym_root)]
        # assert sizes match
        self.assertEqual(len(self.removed_files), len(dead_links))
        # assert contents match
        self.assertEqual(set(self.removed_files), set([os.path.basename(next_file) for next_file in dead_links]))

    def testClean(self):
        maven_public_utils.clean_site(self.test_sym_root)
        # scan all remaining links to ensure they have targets that exist
        for next_file in [os.path.join(self.test_sym_root, f) for f in
                          os.listdir(self.test_sym_root) if
                          os.path.isfile(os.path.join(self.test_sym_root, f))]:
            self.assertTrue(os.path.islink(next_file))
            self.assertTrue(os.path.isfile(os.path.realpath(next_file)))

    def testBuildSiteSymlink(self):
        '''Tests build_site that the sym links exist and are created'''
        maven_public_utils.build_site(root_dir=self.test_root,
                                      source_root_dir=self.test_file_root,
                                      sym_link=True)

        maven_public_utils.build_site(root_dir=self.test_root,
                                      source_root_dir=self.test_file_root,
                                      sym_link=True)

        site_symlink_anc_dir = os.path.join(self.test_root, maven_public_utils.ANC_DIR)
        self.assertTrue(os.path.islink(site_symlink_anc_dir))
