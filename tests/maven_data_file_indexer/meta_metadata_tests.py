'''
Created on August 8, 2015

@author: bstaley

Tests for the metadata indexer.
'''
import os
import unittest
from shutil import rmtree
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from maven_data_file_indexer import utilities as indexer_utilities
from tests.maven_test_utilities import file_system
from maven_utilities import maven_config


class MetaMetadataTests(unittest.TestCase):
    '''Tests computation of metadata for meta files.'''

    def setUp(self):
        self.root_directory = file_system.get_temp_root_dir()

    def tearDown(self):
        rmtree(self.root_directory)
        self.assertFalse(os.path.isdir(self.root_directory))

    def test_filename_regex(self):
        m = maven_config.metadata_regex.match('collection_ngims_l1a_inventory.csv')
        self.assertIsNotNone(m)
        m = maven_config.metadata_regex.match('collection_ngims_context_inventory.csv')
        self.assertIsNotNone(m)
        m = maven_config.metadata_regex.match('collection_ngims_xml_schema.xml')
        self.assertIsNotNone(m)
        m = maven_config.metadata_regex.match('bundle_maven_iuvs_raw.xml')
        self.assertIsNotNone(m)
        m = maven_config.metadata_regex.match('collection_maven_iuvs_calibrated_occultation_inventory.tab')
        self.assertIsNotNone(m)
        m = maven_config.sis_regex.match('iuvs_pds_sis.pdf')
        self.assertIsNotNone(m)

    def run_metadata_generation_check(self, file_name, expected_instrument, expected_level, expected_plan, expected_version, expected_revision):
        metadata = indexer_utilities.get_metadata_for_metadata_file(file_name)
        self.assertEqual(expected_instrument, metadata.instrument)
        self.assertEqual(expected_plan, metadata.plan)
        self.assertEqual(expected_level, metadata.level)
        self.assertEqual(expected_version, metadata.version)
        self.assertEqual(expected_revision, metadata.revision)

    def test_label_metadata_generation(self):
        test_data = [('mvn_ngi_l1a_collection_inventory_20150101T010101.csv', 'ngi', 'l1a', 'metadata', 1, 0),
                     ('mvn_ngi_context_collection_inventory_20150101T010101.csv', 'ngi', 'context', 'metadata', 1, 0),
                     ('mvn_ngi_xml_collection_schema-inventory_20150101T010101.xml', 'ngi', 'xml', 'metadata', 1, 0),
                     ('mvn_iuv_raw_bundle_20150101T010101.xml', 'iuv', 'raw', 'metadata', 1, 0),
                     ('mvn_iuv_calibrated_collection_occultation-inventory_20150101T010101.tab', 'iuv', 'calibrated', 'metadata', 1, 0),
                     ('mvn_ngi_tid_caveats_20150101T010102_v42_r32.pdf.gz', 'ngi', 'tid', 'metadata', 42, 32)]

        file_system.build_test_files_and_structure('some bogus content to get a file size',
                                                   self.root_directory,
                                                   [f for f, _, _, _, _, _ in test_data])
        for test_datum in test_data:
            self.run_metadata_generation_check(file_name=os.path.join(self.root_directory, test_datum[0]),
                                               expected_instrument=test_datum[1],
                                               expected_level=test_datum[2],
                                               expected_plan=test_datum[3],
                                               expected_version=test_datum[4],
                                               expected_revision=test_datum[5])
