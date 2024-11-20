'''
Created on Feb 08, 2016

@author: bstaley
'''
import unittest
import os
from shutil import rmtree
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'

from maven_public import utilities as maven_public_utils
from tests.maven_test_utilities import file_system


class TestPublicOrbit(unittest.TestCase):

    def setUp(self):
        # copy list
        self.source_generators = maven_public_utils.default_source_generators[:]

        self.test_root = file_system.get_temp_root_dir()
        self.site_root = os.path.join(self.test_root, 'site')
        os.mkdir(self.site_root)
        self.data_root = os.path.join(self.test_root, 'data')
        self.orb_data_root = os.path.join(self.data_root, 'anc', 'orb')

        self.test_orb_files = ['maven_orb_rec_140922_150101_v1.bsp',
                               'maven_orb_rec_140922_150101_v1.orb',
                               'maven_orb_rec_150101_150401_v1.bsp',
                               'maven_orb_rec_150101_150401_v1.orb',
                               'maven_orb_rec_150401_150701_v1.bsp',
                               'maven_orb_rec_150401_150701_v1.orb',
                               ]

        # Build ancillary files
        file_system.build_test_files_and_structure(default_file_contents='something to fill the file',
                                                   files_base_dir=self.orb_data_root,
                                                   files_list=self.test_orb_files)

    def tearDown(self):
        rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))
        maven_public_utils.default_source_generators = self.source_generators

    def testPopulateOrbitFiles(self):
        #for next_gen in maven_public_utils.default_source_generators:
        #    if isinstance(next_gen, maven_public_utils.SystemFileQueryMetadata):
        #        next_gen.root_dir = self.anc_data_root

        maven_public_utils.build_site(self.site_root, self.data_root, True)

        # assert all links exist.
        self.assertTrue(os.path.exists(
            os.readlink(os.path.join(self.site_root, maven_public_utils.ORB_DIR))))

        for next_public_file in self.test_orb_files:
            self.assertTrue(os.path.exists(os.path.join(self.site_root, maven_public_utils.ORB_DIR, next_public_file)))
