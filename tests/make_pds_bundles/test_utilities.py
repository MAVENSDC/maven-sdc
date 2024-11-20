'''
Created on Mar 9, 2015

@author: bstaley
'''
import os
import shutil
import unittest
try:
    from . import utilities as test_utilities
except ImportError:
    import utilities as test_utilities
from maven_data_file_indexer import utilities as idx_utils
from tests.maven_test_utilities import file_system
from tests.maven_test_utilities.db_utils import delete_data, insert_ancillary_file_metadata, insert_science_files_metadata
from make_pds_bundles import utilities
from maven_database.models import AncillaryFilesMetadata, ScienceFilesMetadata
from maven_utilities import constants, utilities as utils_utilities
os.environ[constants.python_env] = 'testing'


class Test(unittest.TestCase):

    def setUp(self):
        self.test_root = file_system.get_temp_root_dir()
        self.directory_path = os.path.join(self.test_root, 'path/to/nowhere')
        if not os.path.isdir(self.directory_path):
            os.makedirs(self.directory_path)
        assert os.path.isdir(self.directory_path)

        # Add test files
        self.test_pds_files = ['mvn_euv_l2_bands_20141113_v00_r00.cdf',
                               'mvn_euv_l2_bands_20141113_v01_r00.cdf',
                               'mvn_euv_l2_bands_20141113_v01_r01.cdf',
                               'mvn_euv_l3_bands_20141113_v01_r01.cdf',
                               'mvn_euv_l3a_bands_20141113_v01_r01.cdf',
                               'mvn_lpw_l0b_swp2_20141217_v01_r01.cdf',
                               'mvn_lpw_l0b_swp3_20141217_v01_r01.cdf',
                               'mvn_lpw_l0b_swp3_20141217_v01_r01.txt',
                               'mvn_acc_l3_pro-acc-P00713_20150211_v01_r01.tab',
                               'mvn_euv_20230101_0000.png',
                               'mvn_euv_flare_20230320_1200_M5.4.png',
                               'mvn_euv_flare_catalog_2023.txt'
                               ]

        metadata_for_test_files = test_utilities.get_metadata(self.test_pds_files, self.test_root)
        # Build Directory Structure
        file_system.build_test_files_and_structure("test kp data", self.test_root, [nxt[1] for nxt in metadata_for_test_files])

        # Put into database
        test_utilities.populate_science_metadata([idx_utils.ScFileMetadata(
            os.path.split(nxt[1])[0],
            nxt[0].file_name,
            utils_utilities.get_file_root_plus_extension(nxt[0].file_name, nxt[0].file_name),
            nxt[0].file_size,
            nxt[0].instrument,
            nxt[0].level,
            nxt[0].descriptor,
            nxt[0].timetag,
            nxt[0].mod_date,
            utils_utilities.get_absolute_version(int(nxt[0].version),
                                                 int(nxt[0].revision)),
            int(nxt[0].version),
            int(nxt[0].revision),
            nxt[0].file_extension,
            nxt[0].plan,
            nxt[0].orbit,
            nxt[0].mode,
            nxt[0].data_type,
            nxt[0].flare_class) for nxt in metadata_for_test_files])

        # Test ancillary filenames
        file_size = 1236
        self.anc_files = ['sci_anc_eps14_365_%03d.drf' % i for i in range(10)]
        self.anc_path = []
        for anc in self.anc_files:
            filename_path = os.path.join(self.directory_path, anc)
            os.path.isdir(filename_path)
            self.anc_path.append(filename_path)
            with open(filename_path, 'w') as f:
                f.write('a' * file_size)

        # Test science filenames
        self.sci_files = ['mvn_ins_grp_l0b_20130430_v%03d.dat' % i for i in range(10)]
        self.sci_path = []
        for sci in self.sci_files:
            filename_path = os.path.join(self.directory_path, sci)
            os.path.isdir(filename_path)
            self.sci_path.append(filename_path)
            with open(filename_path, 'w') as f:
                f.write('a' * file_size)

    def tearDown(self):
        delete_data()
        shutil.rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))
        delete_data(AncillaryFilesMetadata, ScienceFilesMetadata)

    def testGetAllScienceFiles(self):
        results = utilities.get_all_science_files()
        self.assertEqual(len(self.test_pds_files), len(results))

    def testGetScienceFilesByLevel(self):
        results = utilities.get_all_science_files(level_list=['l2', 'l3'])
        self.assertEqual(5, len(results))

    def testGetScienceFilesByExt(self):
        results = utilities.get_all_science_files(extension_list=['txt'])
        self.assertEqual(2, len(results))

    def testGetMaxScienceFilesByInstrument(self):
        results = utilities.get_latest_science_files(instrument_list=['euv'])
        self.assertEqual(6, len(results))

    def testQueryAncillaryFiles(self):
        metadata_count = 0
        self.assertEqual(int(AncillaryFilesMetadata.query.count()), metadata_count)
        for anc_file in self.anc_path:
            self.assertEqual(os.stat(anc_file).st_size, 1236)
            metadata = idx_utils.get_metadata_for_ancillary_file(anc_file)
            insert_ancillary_file_metadata(file_name=metadata.file_name,
                                           start_date=metadata.start_date,
                                           end_date=metadata.end_date,
                                           mod_date=metadata.mod_date)
            metadata_count += 1
            self.assertEqual(int(AncillaryFilesMetadata.query.count()), metadata_count)
        anc_results = utilities.get_all_ancillary_files(product_list=None, extension_list=None)
        
        for anc_file in self.anc_path:
            file_found = [s for s in anc_results if os.path.basename(anc_file) in s]
            self.assertNotEqual(file_found, [])
            self.assertEqual(len(file_found), 1)

    def testQueryScienceFiles(self):
        metadata_count = len(self.test_pds_files)  # 9 files currently exist
        self.assertEqual(int(ScienceFilesMetadata.query.count()), metadata_count)  # metadata already includes l2 and l3 values part of self.test_pds_files
        for sci_file in self.sci_path:
            self.assertEqual(os.stat(sci_file).st_size, 1236)
            metadata = idx_utils.get_metadata_for_science_file(sci_file)
            insert_science_files_metadata(file_name=sci_file)
            metadata_count += 1
            self.assertEqual(int(ScienceFilesMetadata.query.count()), metadata_count)
        sci_results = utilities.get_all_science_files(instrument_list=None, extension_list=None)
        for sci_file in self.sci_path:
            self.assertIn(os.path.basename(sci_file), str(sci_results))

    def testGetLastestScience(self):
        query_none_list_files = utilities.get_latest_science_files(instrument_list=None,
                                                                   grouping_list=None,
                                                                   plan_list=None,
                                                                   level_list=None)
        self.assertNotEqual(query_none_list_files, [])

        query_none_list_metadata = utilities.get_latest_science_metadata(instrument_list=None,
                                                                     grouping_list=None,
                                                                     plan_list=None,
                                                                     level_list=None,
                                                                     version=None,
                                                                     revision=None,
                                                                     extension_list=None)
        self.assertNotEqual(query_none_list_metadata, [])
