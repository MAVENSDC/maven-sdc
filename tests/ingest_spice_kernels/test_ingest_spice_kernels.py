import unittest
import os
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
import time
from shutil import rmtree
from tests.maven_test_utilities import file_system
from ingest_spice_kernels import utilities
from ingest_spice_kernels import config
from maven_utilities.utilities import file_is_old_enough
from maven_utilities import file_pattern, anc_config


class IngestSpiceKernelUnittest(unittest.TestCase):

    def setUp(self):
        self.test_root = file_system.get_temp_root_dir()
        self.src_directory = os.path.join(self.test_root, 'src')
        os.mkdir(self.src_directory)
        self.dest_root_directory = os.path.join(self.test_root, 'dest')
        self.dest_directory = self.dest_root_directory + '/lsk'
        os.makedirs(self.dest_directory)
        self.dest_directory = self.dest_root_directory + '/sclk'
        os.makedirs(self.dest_directory)
        self.dest_directory = self.dest_root_directory + '/fk'
        os.makedirs(self.dest_directory)
        self.dest_directory = self.dest_root_directory + '/ck'
        os.makedirs(self.dest_directory)
        config.poc_root_dest_dir = self.dest_root_directory

    def tearDown(self):
        rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))

    def testFileIsOldEnough(self):
        '''ingest_spice_kernels.file_is_old_enough should return
        True for files that are old enough and False for others.
        '''
        fn = 'mvn_spice_kernel.ti'
        path = os.path.join(self.src_directory, fn)
        f = open(path, 'w')
        f.close()
        self.assertFalse(file_is_old_enough(path, config.age_limit))
        os.utime(path, (time.time(), time.time() - config.age_limit - 0.1))
        self.assertTrue(file_is_old_enough(path, config.age_limit))

    def testGetSrcFileNames(self):
        '''ingest_spice_kernels.get_src_file_names should return the
        correct list of files. This list should include spice kernel
        file names but not others.
        '''
        good_fn = 'mvn_spice_kernel.ti'
        path = os.path.join(self.src_directory, good_fn)
        f = open(path, 'w')
        f.close()
        bogus_fn = 'something_bogus.dat'
        path = os.path.join(self.src_directory, bogus_fn)
        f = open(path, 'w')
        f.close()
        good_return_fns, bad_return_fns = utilities.split_files(self.src_directory)
        self.assertTrue(good_fn in good_return_fns)
        self.assertFalse(good_fn in bad_return_fns)
        self.assertTrue(bogus_fn in bad_return_fns)
        self.assertFalse(bogus_fn in good_return_fns)

    def testGetDestinationPath(self):
        '''ingest_spice_kernels.get_destination_path should return
        the correct path based on the file's type.
        '''
        fn = 'mvn_spice_kernel.tls'
        dest_path = config.anc_spice_general_loc_generation(file_pattern.matches_on_group([anc_config.anc_spice_general_regex], fn))
        correct_dest_path = os.path.join(self.dest_root_directory + '/lsk')
        self.assertEqual(correct_dest_path, dest_path)

        fn = 'mvn_spice_kernel.tsc'
        dest_path = config.anc_spice_general_loc_generation(file_pattern.matches_on_group([anc_config.anc_spice_general_regex], fn))
        correct_dest_path = os.path.join(self.dest_root_directory + '/sclk')
        self.assertEqual(correct_dest_path, dest_path)

        fn = 'MVN_SCLKSCET.00045.tsc' # optional center values
        dest_path = config.anc_spice_general_loc_generation(file_pattern.matches_on_group([anc_config.anc_spice_general_regex], fn))
        correct_dest_path = os.path.join(self.dest_root_directory + '/sclk')
        self.assertEqual(correct_dest_path, dest_path)

        fn = 'mvn_spice_kernel.tpc'
        dest_path = config.anc_spice_general_loc_generation(file_pattern.matches_on_group([anc_config.anc_spice_general_regex], fn))
        correct_dest_path = os.path.join(self.dest_root_directory + '/pck')
        self.assertEqual(correct_dest_path, dest_path)

        fn = 'mvn_spice_kernel.bsp'
        dest_path = config.anc_spice_general_loc_generation(file_pattern.matches_on_group([anc_config.anc_spice_general_regex], fn))
        correct_dest_path = os.path.join(self.dest_root_directory + '/spk')
        self.assertEqual(correct_dest_path, dest_path)

        fn = 'mvn_spice_kernel.bc'
        dest_path = config.anc_spice_general_loc_generation(file_pattern.matches_on_group([anc_config.anc_spice_general_regex], fn))
        correct_dest_path = os.path.join(self.dest_root_directory + '/ck')
        self.assertEqual(correct_dest_path, dest_path)

        fn = 'mvn_spice_kernel.ti'
        dest_path = config.anc_spice_general_loc_generation(file_pattern.matches_on_group([anc_config.anc_spice_general_regex], fn))
        correct_dest_path = os.path.join(self.dest_root_directory + '/ik')
        self.assertEqual(correct_dest_path, dest_path)

        fn = 'mvn_spice_kernel.tf'
        dest_path = config.anc_spice_general_loc_generation(file_pattern.matches_on_group([anc_config.anc_spice_general_regex], fn))
        correct_dest_path = os.path.join(self.dest_root_directory + '/fk')
        self.assertEqual(correct_dest_path, dest_path)

        fn = 'mvn_spice_kernel.something_bogus'
        with self.assertRaises(AssertionError):
            dest_path = config.anc_spice_general_loc_generation(file_pattern.matches_on_group([anc_config.anc_spice_general_regex], fn))

    def testIUVSpattern(self):
        #Check the IUVS pattern
        iuv_one_day_fn = 'mvn_iuv_all_l0_20170325.bc'
        iuv_one_month_fn = 'mvn_iuv_all_l0_170325_170425_v01.bc'
        non_iuv_fn = 'mvn_app_red_180325.bc'

        iuvs_rules = config.file_rules[0]
        
        #Check the correct file patterns
        m_day = file_pattern.matches_on_group(iuvs_rules.patterns, iuv_one_day_fn)
        self.assertTrue(m_day is not None)
        m_month = file_pattern.matches_on_group(iuvs_rules.patterns, iuv_one_month_fn)
        self.assertTrue(m_month is not None)
        correct_dest_path = os.path.join(self.dest_root_directory + '/ck')
        self.assertEqual(iuvs_rules.absolute_directories(m_day), correct_dest_path)
        self.assertEqual(iuvs_rules.absolute_directories(m_month), correct_dest_path)
        
        #Verify the one month and one day regexes are not confused
        m = file_pattern.matches_on_group([iuvs_rules.patterns[1]], iuv_one_day_fn)
        self.assertTrue(m is None)
        m = file_pattern.matches_on_group([iuvs_rules.patterns[0]], iuv_one_month_fn)
        self.assertTrue(m is None)
        
        #Check the incorrect file pattern
        m = file_pattern.matches_on_group(iuvs_rules.patterns, non_iuv_fn)
        self.assertTrue(m is None)