'''
Created on Mar 2, 2017

@author: cosc3564
'''
import unittest
import os
from shutil import rmtree
from maven_utilities import constants, maven_config
os.environ[constants.python_env] = 'testing'
from maven_data_file_indexer import utilities
from tests.maven_test_utilities import db_utils
from tests.maven_test_utilities import file_names
from maven_database.models import ScienceFilesMetadata, AncillaryFilesMetadata
from tests.maven_test_utilities.file_system import get_temp_root_dir


class TestMavenDataFileIndexerUtilities(unittest.TestCase):


    def setUp(self):
        self.test_root = get_temp_root_dir()
        self.src_directory = os.path.join(self.test_root, 'src')
        os.mkdir(self.src_directory)

    def tearDown(self):
        rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))
        db_utils.delete_data(ScienceFilesMetadata, AncillaryFilesMetadata)
    
    def testYDOYtoDateTimeConverter(self):
        '''Test year and day-of-year are converted into UTC datetime at the start of the day
        '''
        datetime_compare = '2017-05-05 00:00:00+00:00'

        # test if the time is between 0-365
        year = 2017
        day = 125
        localized = utilities.convert_ydoy_to_datetime(year, day)
        self.assertEqual(str(localized), datetime_compare)
        
        # test if time is greater than 365 (rolls over through months, but stays within the same year)
        day = 1220
        while (day > 365):
            day -= 365
        localized = utilities.convert_ydoy_to_datetime(int(year), int(day))
        self.assertEqual(str(localized), datetime_compare)
    
    def testScienceMetadata(self):
        '''Test that the science files metadata are inserted as metadata
        '''
        test_description = 'test-science-metadata-regex-exists'
        test_filename = file_names.generate_science_file_name(description=test_description)
        
        db_utils.insert_science_files_metadata(file_name=test_filename,
                                               file_size=0)

        found_science_metadata = utilities.is_science_metadata(test_filename)
        self.assertTrue(found_science_metadata)

    def testAncillaryMetadata(self):
        '''Test that the ancillary files metadata are inserted as metadata
        '''
        db_utils.create_ancillary_metadata()
        anc = AncillaryFilesMetadata.query.first()
        found_ancillary_metadata = utilities.is_ancillary_metadata(anc.file_name)
        self.assertTrue(found_ancillary_metadata)
    
    def testGenerateMetadataQL(self):
        '''Test that the ql metadata is generated from the ql/summary plot files
        '''
        ql_filename = os.path.join(self.src_directory, 'mvn_test_ql_20070516.png')
        os.mkdir(ql_filename)
        self.assertIsNotNone(maven_config.ql_regex.match(os.path.basename(ql_filename)), 'does not match regex')
        ql = utilities.generate_metadata_for_ql_file([ql_filename])
        ql_metadata = list(ql)
        self.assertNotEqual(ql_metadata, [])
        self.assertIn(os.path.basename(ql_filename), [x[1] for x in ql_metadata][0])
        self.assertIn(os.path.dirname(ql_filename), [x[0] for x in ql_metadata][0])
    
    def testGenerateMetadataforMetadata(self):
        '''Test that metadata for metadata is generated for a given metadata file
        '''
        metadata_filename = os.path.join(self.src_directory, 'mvn_iuv_raw_collection_limb-inventory_20140711T224001.tab')
        os.mkdir(metadata_filename)
        self.assertIsNotNone(maven_config.metadata_index_regex.match(os.path.basename(metadata_filename)), 'does not match regex')
        md = utilities.generate_metadata_for_metadata_file([metadata_filename])
        md_metadata = list(md)
        self.assertNotEqual(md_metadata, [])
        self.assertIn(os.path.basename(metadata_filename), [x[1] for x in md_metadata][0])
        self.assertIn(os.path.dirname(metadata_filename), [x[0] for x in md_metadata][0])