'''
Created on Jun 5, 2015

@author: tbussell

Tests for the combined l0 file and Science file utilities used in the file indexer.
'''
from io import StringIO
import logging
import os
import unittest
from shutil import rmtree
import sys
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from maven_data_file_indexer.audit_utilities import get_metadata_from_disk
from maven_data_file_indexer.maven_file_indexer import run_full_index
from tests.maven_test_utilities.file_system import get_temp_root_dir
from tests.maven_test_utilities.db_utils import insert_science_files_metadata, delete_data
from maven_database.models import ScienceFilesMetadata

logger = logging.getLogger('maven.maven_data_file_indexer.l0_and_science_files_methods_tests.log')
class l0SciFiInsertTestCase(unittest.TestCase):

    def setUp(self):
        self.test_root_dir = get_temp_root_dir()
        self.test_fn_list = [
            ('db',
             ['mvn_ins_grp_l0_20130430_v01.dat', 0, 1, 0, 'dat']),
            ('disk',
             ['mvn_ins_grp_l0_22221111_v01.dat', 0, 1, 0, 'dat']),
            ('both',
             ['mvn_kp_insitu_20130430T010203_v01_r02.tab', 0, 1, 2, 'tab']),
            ('both',
             ['mvn_kp_insitu_22221111T010203_v01_r02.tab', 0, 1, 2, 'tab']),
            ('both',
             ['mvn_sep_anc_20140708_v01_r00.cdf', 0, 1, 0, 'cdf']),
            ('both',
             ['mvn_euv_l2b_orbit_merged_v08_r01.sav', 0, 1, 0, 'sav']),
            ('both',
             ['mvn_acc_l3_pro-acc-P00713_20150211_v01_r01.tab', 0, 1, 0, 'tab'],
             ), 
             ('both',
              ['mvn_euv_20230101_0000.png', 0, 1, 0, 'png']),
              ('both',
              ['mvn_euv_flare_20230320_1200_M5.4.png', 0, 1, 0, 'png']),
              ('both',['mvn_euv_flare_catalog_2023.txt', 0, 1, 0, 'txt'])
        ]

        for f in [x[1] for x in self.test_fn_list if x[0] == 'disk' or x[0] == 'both']:
            full_path = os.path.join(self.test_root_dir, f[0])

            with open(full_path, 'w') as _:
                pass
            f[1] = os.path.getsize(full_path)

            assert(os.path.isfile(full_path))

        for insert_item in [x[1] for x in self.test_fn_list if x[0] == 'db' or x[0] == 'both']:

            insert_science_files_metadata(insert_item[0],
                                          dir_path=self.test_root_dir,
                                          file_size=insert_item[1],
                                          version=insert_item[2],
                                          revision=insert_item[3],
                                          extension=insert_item[4])

        self.both_truth = [x[1] for x in self.test_fn_list if x[0] == 'both']
        self.disk_only_truth = [x[1] for x in self.test_fn_list if x[0] == 'disk']
        self.db_only_truth = [x[1] for x in self.test_fn_list if x[0] == 'db']

    def tearDown(self):
        delete_data(ScienceFilesMetadata)
        rmtree(self.test_root_dir)
        self.assertFalse(os.path.isdir(self.test_root_dir))

    def test_get_l0_and_scifi_names_on_disk(self):
        fs_metadata = get_metadata_from_disk(self.test_root_dir)

        self.assertEqual(len(self.disk_only_truth) + len(self.both_truth), len(fs_metadata))

        disk_both_pname_truth = [os.path.join(self.test_root_dir, x[0]) for x in self.disk_only_truth]
        disk_both_pname_truth.extend([os.path.join(self.test_root_dir, x[0]) for x in self.both_truth])
        for _next in fs_metadata:
            self.assertIn(_next.path_name, disk_both_pname_truth)

    def test_update_l0_and_science_files(self):
        bogus = insert_science_files_metadata('mvn_euv_l2b_orbit_merged_v09_r01.sav',
                                              dir_path=self.test_root_dir,
                                              file_size=42,
                                              version=1,
                                              revision=2,
                                              extension='.ext')

        query1 = ScienceFilesMetadata.query.all()
        self.assertIn(bogus, query1)
        run_full_index([self.test_root_dir])
        query2 = ScienceFilesMetadata.query.all()
        # TODO assertItemsEqual is gone!
        #self.assertItemsEqual((x[1][0] for x in self.test_fn_list if x[0] == 'disk' or x[0] == 'both'),
        #                      (m.file_name for m in query2))

    def test_remove_lost_metadata(self):
        for _next in self.db_only_truth:
            cnt = ScienceFilesMetadata.query.filter(ScienceFilesMetadata.file_name == _next[0]).count()
            self.assertEqual(1, cnt)
        run_full_index([self.test_root_dir])
        for _next in self.db_only_truth:
            cnt = ScienceFilesMetadata.query.filter(ScienceFilesMetadata.file_name == _next[0]).count()
            self.assertEqual(0, cnt)

    def test_dry_run(self):
        # This tests ensures that there is simply any kind of output to the correct log
        # when performing a dry run.  There should be, since in setup there is a file not 
        # in the database, and a row in the database that is not an existing file.  
        
        # Configure the logger for the test
        logger_name = 'maven.maven_data_file_indexer.maven_data_file_indexer.log'
        test_logger = logging.getLogger(logger_name)
        test_logger.setLevel(logging.DEBUG)  # Ensure logger is set to capture DEBUG messages

        # Use a list to collect log output for verification
        log_output = []

        # Create a custom log handler; all it does is append the log to the log_output list
        class ListHandler(logging.Handler):
            def emit(self, record):
                log_output.append(self.format(record))
        
        list_handler = ListHandler()
        test_logger.addHandler(list_handler)
        
        try: 
            run_full_index([self.test_root_dir], dry_run=True)
            self.assertTrue(len(log_output) > 0, "No log messages were captured.")
        finally:
            # Clean up the handler
            test_logger.removeHandler(list_handler)