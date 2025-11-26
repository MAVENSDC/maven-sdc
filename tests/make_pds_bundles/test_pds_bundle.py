"""
Created on Mar 3, 2015

@author: bstaley, bharter
"""

import unittest
import pytest
import shutil
import re
import os
try:
    from tests.make_pds_bundles import utilities as test_utilities
except ImportError:
    import utilities as test_utilities
import tarfile
import smtplib
import csv
import gzip
from dateutil.parser import parse
from maven_database.models import PdsArchiveRecord
from maven_ops_database.database import init_db
from make_pds_bundles import config, make_pds_bundles
from make_pds_bundles.results import GENERATION_SUCCESS
from make_pds_bundles.make_pds_bundles import direct_out_logger, print_instrument_config
from tests.maven_test_utilities import file_system, db_utils, log_handlers
from tests.maven_test_utilities import mail_utilities
from maven_utilities.utilities import is_compressed_format
from maven_utilities import maven_log, time_utilities, constants
os.environ[constants.python_env] = 'testing'

# Setup in-memory database
init_db()


class TestPdsBundles(unittest.TestCase):
    '''
    Unit tests for the make_pds_bundles.make_pds_bundles package.  Tests will cover
    the ability to find files to archive, archive and zip the archived files.  Tests
    will also cover the generation of the PDS manifest file
    '''

    # Remove ability to send emails
    smtplib.SMTP = mail_utilities.DummySMTP

    test_handler = log_handlers.RecordHandler()

    # The dates are reversed to test that "run_archive" will put the dates in the correct order
    test_start = '2015-01-01'
    test_end = '2014-01-01'

    test_success_files = ['mvn_euv_l2_bands_20141018_v01_r00.cdf',
                          'mvn_euv_l2_bands_20141018.xml',
                          'mvn_lpw_l2_lpiv_20141006_v01_r01.cdf',
                          'mvn_lpw_l2_lpiv_20141006.xml',
                          'mvn_lpw_l2_we12_20141008_v01_r01.cdf',
                          'mvn_lpw_l2_we12_20141008.xml',
                          'mvn_swe_l2_svypad_20140627_v01_r01.cdf',
                          'mvn_swe_l2_svypad_20140627.xml',
                          'mvn_swi_l2_coarsearc3d_20140320_v01_r03.cdf',
                          'mvn_swi_l2_coarsearc3d_20140320.xml',
                          'mvn_swi_l2_finesvy3d_20140319_v01_r03.cdf',
                          'mvn_swi_l2_finesvy3d_20140319.xml',
                          'mvn_swi_l2_onboardsvymom_20140319_v01_r03.cdf'
                          ]
    metadata_files = ['collection_ngims_l1b_inventory.xml',
                      'collection_ngims_l1b_inventory.csv',
                      'collection_ngims_document.xml',
                      'collection_ngims_document.csv',
                      'collection_ngims_xml_schema.xml',
                      'collection_ngims_xml_schema.tab',
                      'ngims_pds_sis.xml',
                      'ngims_pds_sis.pdf']
    test_success_compressed_files = ['mvn_swi_l2_onboardsvymom_20140319.xml.gz']
    test_miss_files = [
        'mvn_euv_l0_bands_20141018_v00_r00.cdf',  # l0
        'mvn_euv_l2_bands_20141018_v00_r00.cdf',  # v00
        'mvn_sta_l2_d0-32e4d16a8m_20141206_v00_r00.cdf'  # 32e4d16a8m is captured as the orbit# not the plan
    ]

    def setUp(self):
        maven_log.config_logging()
        direct_out_logger.addHandler(self.test_handler)
        self.test_root = file_system.get_temp_root_dir()
        os.makedirs(os.path.join(self.test_root, 'mavenpro'))
        post_fix = 'maven/data/sci'
        self.test_root_maven = os.path.join(self.test_root, post_fix)
        os.makedirs(self.test_root_maven)
        assert os.path.isdir(self.test_root_maven)

        self.metadata_for_test_files = test_utilities.get_metadata(self.test_success_files + 
                                                                   self.test_miss_files + 
                                                                   self.test_success_compressed_files,
                                                                   self.test_root_maven)

        # Put into database
        test_utilities.populate_science_metadata(next_data[0] for next_data in self.metadata_for_test_files)
        
        file_system.build_test_files_and_structure('some test data', self.test_root, [f[1] for f in self.metadata_for_test_files])
        metadata_path = os.path.join(self.test_root, 'maven/data/sci/ngi/metadata')
        file_system.build_test_files_and_structure('some test data', metadata_path, self.metadata_files)

        for next_file in [f[1] for f in self.metadata_for_test_files if os.path.basename(f[1]) in self.test_success_compressed_files]:
            with gzip.open(next_file, 'wb') as gz:
                gz.write(b'filling up gzip_file')

        # Add some ancillary meta data
        self.generated_anc_files = db_utils.create_ancillary_metadata(product_list=['prod1', 'prod2'], root_dir=self.test_root_maven, year_list=['14'])
        file_system.build_test_files_and_structure("some anc data", self.test_root_maven, self.generated_anc_files)

    def tearDown(self):
        direct_out_logger.removeHandler(self.test_handler)
        self.test_handler.clear()
        db_utils.delete_data()
        shutil.rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))

    def run_result_generation(self, config_keys, dry_run, expected_success):
        make_pds_bundles.run_archive(self.test_start, self.test_end, config_keys, self.test_root, dry_run)
        results = PdsArchiveRecord.query.all()
        self.assertEqual(expected_success, len(results))

        success_cnt = len([x for x in results if x.generation_result == GENERATION_SUCCESS])
        self.assertEqual(expected_success, success_cnt)

    def test_unzipped_compressed(self):     
        test_file_names = [f[1] for f in self.metadata_for_test_files]
        compressed_file = [f for f in test_file_names if '.gz' in f]
        self.assertTrue(len(compressed_file) == 1)
        self.assertTrue(is_compressed_format(compressed_file[0]))
        
        make_pds_bundles.run_archive(self.test_start, self.test_end, [config.all_key], self.test_root, True)
        
        file_loc_pattern = re.compile(r'.*/data/sci/([a-zA-Z]{3})/.*')
        
        for test_file in [f[1] for f in self.metadata_for_test_files if os.path.basename(f[1]) in self.test_success_compressed_files]:
            file_loc_match = file_loc_pattern.match(test_file)
            instrument = file_loc_match.groups()[0]
            bundle_file_location = os.path.join(self.test_root, 'maven/data/arc/', instrument)
            self.assertFalse(is_compressed_format([os.path.join(bundle_file_location, f) for f in os.listdir(bundle_file_location)][0]))

    def test_results_dry_run(self):
        self.run_result_generation([config.all_key], True, 52)

    def test_results_single_instrument(self):
        self.run_result_generation(['euv'], False, 1)

    def test_main_manifest(self):
        make_pds_bundles.run_archive(self.test_start, self.test_end, [config.all_key], self.test_root, True)

        file_loc_pattern = re.compile(r'.*/data/sci/([a-zA-Z]{3})/.*')

        # iterate over all the test files we expect to have archived
        for test_file in [f[1] for f in self.metadata_for_test_files if os.path.basename(f[1]) in self.test_success_files and f[0].file_extension == 'xml']:
            file_loc_match = file_loc_pattern.match(test_file)
            instrument = file_loc_match.groups()[0]
            manifest_location = os.path.join(self.test_root, 'maven/data/arc', instrument)
            # assert the manifest was created
            self.assertTrue(len([f for f in os.listdir(manifest_location) if f.startswith('transfer')]) == 1)
            manifest_file = [f for f in os.listdir(manifest_location) if f.startswith('transfer')][0]
            manifest_file = os.path.join(manifest_location, manifest_file)
            with open(manifest_file, 'r') as f:
                # assert instrument file made it into manifest
                file_without_ext = os.path.splitext(os.path.split(test_file)[1])[0]
                self.assertTrue(file_without_ext in f.read(), 'Could not find %s in %s' % (file_without_ext, manifest_file))

    def test_main_tarball(self):
        make_pds_bundles.run_archive(self.test_start, self.test_end, [config.all_key], self.test_root, False)

        file_loc_pattern = re.compile(r'.*/data/sci/([a-zA-Z]{3})/.*')

        # iterate over all the test files we expect to have archived
        for test_file in [f[1] for f in self.metadata_for_test_files if os.path.basename(f[1]) in self.test_success_files]:
            file_loc_match = file_loc_pattern.match(test_file)
            instrument = file_loc_match.groups()[0]
            tgz_location = os.path.join(self.test_root, 'maven/data/arc', instrument)
            # assert the tgz was created
            self.assertTrue(len([f for f in os.listdir(tgz_location) if '.tgz' in f]) == 1)
            tgz_file = [f for f in os.listdir(tgz_location) if '.tgz' in f][0]
            tgz_file = os.path.join(tgz_location, tgz_file)
            with tarfile.open(tgz_file, 'r:gz') as opened_tgz_file:
                test_file_without_path = os.path.split(test_file)[1]
                self.assertTrue(test_file_without_path in [os.path.split(f.name)[1] for f in opened_tgz_file], 'File %s was not in the tgz %s' % (test_file_without_path, [f.name for f in opened_tgz_file]))

    def test_metadata_tarball(self):
        make_pds_bundles.run_archive(self.test_start, self.test_end, ['ngi-meta'], self.test_root, False)

        # iterate over all the test files we expect to have archived
        instrument = 'ngi'
        tgz_location = os.path.join(self.test_root, 'maven/data/arc/', instrument)
        # assert the tgz was created
        self.assertTrue(len([f for f in os.listdir(tgz_location) if '.tgz.1' in f]) == 1)
        tgz_file = [f for f in os.listdir(tgz_location) if '.tgz.1' in f][0]
        tgz_file = os.path.join(tgz_location, tgz_file)
        with tarfile.open(tgz_file, 'r:gz') as opened_tgz_file:
            assert len(opened_tgz_file.getnames())==len(self.metadata_files)
            for f in opened_tgz_file:
                self.assertTrue(os.path.split(f.name)[1] in self.metadata_files)

    def test_event_tarball(self):
        make_pds_bundles.run_archive(self.test_start, self.test_end, [config.all_key], self.test_root, False)
 
        tgz_location = os.path.join(self.test_root, 'maven/data/arc', 'events')
        # assert the tgz was created
        self.assertTrue(len([f for f in os.listdir(tgz_location) if '.tgz' in f]) == 1)
        # assert the csv was created
        self.assertTrue(len([f for f in os.listdir(tgz_location) if '.csv' in f]) == 2)
 
        tgz_file = [f for f in os.listdir(tgz_location) if '.tgz' in f][0]
        tgz_file = os.path.join(tgz_location, tgz_file)
 
        csv_files = [f for f in os.listdir(tgz_location) if '.csv' in f]
 
        with tarfile.open(tgz_file, 'r:gz') as opened_tgz_file:
            for csv_file in csv_files:
                self.assertTrue(csv_file in [os.path.split(f.name)[1] for f in opened_tgz_file], 'File %s was not in the tgz %s' % (csv_file, [f.name for f in opened_tgz_file]))

    def test_anc_manifest(self):
        config.instrument_config['anc'] = config.ScienceFileSearchParameters(instrument='anc',
                                    levels=[],
                                    plans=['prod1','prod2'],
                                    groups=[],
                                    descs=[],
                                    exts=['drf', 'txt', 'sff'],
                                    ver=None,
                                    rev=None,
                                    file_name=None,
                                    as_inv_file=False,
                                    uprev_inv_file=False,
                                    label_ver=None,
                                    label_rev=None)
        make_pds_bundles.run_archive(self.test_start, self.test_end, [config.ancillary_key], self.test_root, False)

        manifest_location = os.path.join(self.test_root, 'maven/data/arc', 'anc')
        self.assertTrue(len([f for f in os.listdir(manifest_location) if f.startswith('transfer')]) == 1)
        manifest_file = [f for f in os.listdir(manifest_location) if f.startswith('transfer')][0]
        manifest_file = os.path.join(manifest_location, manifest_file)

        with open(manifest_file, 'r') as f:
            contents = f.read()
            for drf_full_file in self.generated_anc_files:
                drf_file = os.path.basename(drf_full_file)
                # assert file made it into manifest
                file_without_ext = os.path.splitext(drf_file)[0]
                self.assertTrue(file_without_ext in contents, 'Could not find %s in %s' % (file_without_ext, manifest_file))

    def test_anc_tarball(self):
        make_pds_bundles.run_archive(self.test_start, self.test_end, [config.all_key], self.test_root, False)
 
        tgz_location = os.path.join(self.test_root, 'maven/data/arc', 'anc')
        # assert the tgz was created
        self.assertTrue(len([f for f in os.listdir(tgz_location) if '.tgz' in f]) == 1)
 
        tgz_file = [f for f in os.listdir(tgz_location) if '.tgz' in f][0]
        tgz_file = os.path.join(tgz_location, tgz_file)
 
        with tarfile.open(tgz_file, 'r:gz') as opened_tgz_file:
            for drf_full_file in self.generated_anc_files:
                drf_file = os.path.split(drf_full_file)[1]
                self.assertTrue(drf_file in [os.path.split(f.name)[1] for f in opened_tgz_file], 'File %s wasnt in the tgz %s' % (drf_file, [f.name for f in opened_tgz_file]))

    @pytest.mark.skip(reason="skipping this test")
    def test_override_config_file(self):
        override_config_file_source = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test_config.py')
        override_config_file_destination = os.path.join(self.test_root, 'maven', 'test_config.py')
        shutil.copyfile(override_config_file_source, override_config_file_destination)
        make_pds_bundles.run_archive(self.test_start, self.test_end, ['swi'], self.test_root, False, override_inst_config=override_config_file_destination)

        file_loc_pattern = re.compile(r'.*/data/sci/([a-zA-Z]{3})/.*')

        ignored_files = ['mvn_swi_l2_coarsearc3d_20140320_v01_r03.cdf',
                         'mvn_swi_l2_finesvy3d_20140319_v01_r03.cdf',
                         'mvn_swi_l2_onboardsvymom_20140319_v01_r03.cdf']

        # iterate over all the test files we expect to have archived
        for test_file in [f[1] for f in self.metadata_for_test_files if os.path.basename(f[1]) in self.test_success_files]:
            file_loc_match = file_loc_pattern.match(test_file)
            instrument = file_loc_match.groups()[0]
            tgz_location = os.path.join(self.test_root, 'maven/data/arc', instrument)
            # assert the tgz was created
            self.assertTrue(len([f for f in os.listdir(tgz_location) if '.tgz' in f]) == 1)
            tgz_file = [f for f in os.listdir(tgz_location) if '.tgz' in f][0]
            tgz_file = os.path.join(tgz_location, tgz_file)
            with tarfile.open(tgz_file, 'r:gz') as opened_tgz_file:
                test_file_without_path = os.path.split(test_file)[1]
                if test_file_without_path in ignored_files:
                    self.assertTrue(test_file_without_path not in [os.path.split(f.name)[1] for f in opened_tgz_file], 'File %s was included in the tgz %s and should not have been' % (test_file_without_path, [f.name for f in opened_tgz_file]))
                else:
                    self.assertTrue(test_file_without_path in [os.path.split(f.name)[1] for f in opened_tgz_file], 'File %s was not in the tgz %s' % (test_file_without_path, [f.name for f in opened_tgz_file]))

    def test_run_report(self):
        make_pds_bundles.run_report(self.test_start, self.test_end, [config.all_key])

        # Assert all files in archive were logged
        for f in self.test_success_files:
            self.assertTrue(self.test_handler.contains(f))

    def test_run_instrument_filters(self):
        override_config_dir = os.path.join(self.test_root, 'maven')
        override_config_file = os.path.join(override_config_dir, 'test_config.py')
        with open(override_config_file, 'w+') as override_file:
            override_file.write('from collections import namedtuple')
            override_file.write("\nScienceFileSearchParameters = namedtuple('ScienceFileSearchParameters',\
                                         ['instrument',\
                                          'levels',\
                                          'plans',\
                                          'groups',\
                                          'exts',\
                                          'file_name',\
                                          'break_search_on_purpose'])")
        with self.assertRaises(Exception) as context:
            make_pds_bundles.get_instrument_filters([config.event_key], override_config_file)
        self.assertTrue('Error: Science File Parameters do not match!' in str(context.exception))
        
    def test_create_event_archive_bundle(self):
        end_dt = time_utilities.to_utc_tz(parse(self.test_end))
        start_dt = time_utilities.to_utc_tz(parse(self.test_start))
        generate_archive = make_pds_bundles.create_event_archive_bundle(os.path.join(self.test_root, 'archive'),
                                                                        end_dt,
                                                                        start_dt,
                                                                        'bundle.py',
                                                                        'manifest.py',
                                                                        'checksum.py',
                                                                        1,
                                                                        False)
        self.assertIsNone(generate_archive)
    
    def test_print_instrument_config(self):
        all_keys = print_instrument_config(config.all_key)
        self.assertIsNone(all_keys)
