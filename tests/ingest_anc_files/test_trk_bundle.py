'''
Unit tests for TRK bundle generation

Created on Sep 25, 2015
@author: bstaley
'''
import unittest
import os
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from shutil import rmtree

from maven_utilities import time_utilities
from tests.maven_test_utilities import file_system, db_utils
from ingest_anc_files import build_trk_bundle
from maven_data_file_indexer import utilities as mdfi_utils


class TestTrkBundle(unittest.TestCase):

    def setUp(self):
        self.test_root = file_system.get_temp_root_dir()
        self.test_src = os.path.join(self.test_root, 'src')
        os.makedirs(self.test_src)
        self.test_out = os.path.join(self.test_root, 'out')
        os.makedirs(self.test_out)
        # hijack the shelve
        self.manifest_file = os.path.join(self.test_root, '.test_trk_manifest')

        self.test_trk_files_2014 = ['143630140SC202DSS35_noHdr.234',
                                    '143641305SC202DSS54_noHdr.234',
                                    '143641735SC202DSS25_noHdr.234',
                                    '143650955SC202DSS55_noHdr.234']
        self.test_trk_files_2015 = [
            '150010155SC202DSS35_noHdr.234',
            '150010955SC202DSS55_noHdr.234',
            '150020130SC202DSS45_noHdr.234']
        self.test_trk_files = self.test_trk_files_2014 + self.test_trk_files_2015

        file_system.build_test_files_and_structure('Just some text to fill the file',
                                                   self.test_src,
                                                   self.test_trk_files)

        for _next in [os.path.join(self.test_src, fname) for fname in self.test_trk_files]:
            meta = mdfi_utils.get_metadata_for_ancillary_file(_next)
            mdfi_utils.insert_ancillary_file_metadatum(meta)

    def tearDown(self):
        rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))
        db_utils.delete_data()

    def testTimespanNoFiles(self):
        '''Test a timespan that contains no TRK files '''
        # test no files
        start_dt = build_trk_bundle.get_dt_from_yydoy(2013, 200)
        end_dt = build_trk_bundle.get_dt_from_yydoy(2013, 220)

        build_trk_bundle.build_bundle(start_dt,
                                      end_dt,
                                      self.test_out,
                                      self.manifest_file)

        self.assertEqual(1, len(os.listdir(self.test_out)))

        manifest = build_trk_bundle.get_manifest(manifest_file=self.manifest_file)
        manifest_keys = list(manifest.keys())

        self.check_manifest(manifest_keys[0], manifest, [], start_dt, end_dt)

    def testTimespanAllFiles(self):
        '''Test a timespan that contains all TRK files '''
        start_dt = build_trk_bundle.get_dt_from_yydoy(2013, 200)
        end_dt = build_trk_bundle.get_dt_from_yydoy(2015, 220)

        build_trk_bundle.build_bundle(start_dt,
                                      end_dt,
                                      self.test_out,
                                      self.manifest_file)

        self.assertEqual(1, len(os.listdir(self.test_out)))

        manifest = build_trk_bundle.get_manifest(manifest_file=self.manifest_file)
        manifest_keys = list(manifest.keys())

        self.check_manifest(manifest_keys[0],
                            manifest,
                            [os.path.join(self.test_src, next_file) for next_file in self.test_trk_files],
                            start_dt,
                            end_dt)

    def testLatest(self):
        '''Test the ability to generate the latest TRK bundle '''
        start_dt = build_trk_bundle.get_dt_from_yydoy(2013, 200)
        end_dt = build_trk_bundle.get_dt_from_yydoy(2015, 1)

        build_trk_bundle.build_bundle(start_dt,
                                      end_dt,
                                      self.test_out,
                                      self.manifest_file)

        self.assertEqual(1, len(os.listdir(self.test_out)))
        manifest = build_trk_bundle.get_manifest(manifest_file=self.manifest_file, writeback=False)
        manifest_keys = list(manifest.keys())

        self.check_manifest(manifest_keys[0],
                            manifest,
                            [os.path.join(self.test_src, next_file) for next_file in self.test_trk_files_2014],
                            start_dt,
                            end_dt)
        
        manifest.close()

        new_end_dt = time_utilities.utc_now()
        build_trk_bundle.build_bundle_latest(new_end_dt, self.test_out, 10000, self.manifest_file)

        self.assertEqual(2, len(os.listdir(self.test_out)))

        manifest = build_trk_bundle.get_manifest(manifest_file=self.manifest_file, writeback=False)

        latest_key = sorted(manifest.keys())[-1]

        self.check_manifest(latest_key,
                            manifest,
                            [os.path.join(self.test_src, next_file) for next_file in self.test_trk_files_2015],
                            end_dt,
                            new_end_dt)
        
        num_bundles = len(manifest)
        
        manifest.close()
        
        '''Test when end_dt is less than the last_generation_dt in build_bundle_latest
           Will not build bundle'''
        recent_end_dt = build_trk_bundle.get_dt_from_yydoy(2014, 200)
        build_trk_bundle.build_bundle_latest(recent_end_dt, self.test_out, 10000, self.manifest_file)
        
        manifest_no_update = build_trk_bundle.get_manifest(manifest_file=self.manifest_file)
        # does not run build_bundles for new time stamp
        self.assertEqual(len(manifest_no_update), num_bundles)

    def testBuildBundleFull(self):
        '''Test the ability to generate the full built bundle'''
        end_dt = build_trk_bundle.get_dt_from_yydoy(2015, 1)
        build_trk_bundle.build_bundle_full(end_dt, self.test_out, 1000, self.manifest_file)
        self.assertEqual(1, len(os.listdir(self.test_out)))
        
        manifest = build_trk_bundle.get_manifest(manifest_file=self.manifest_file)
        manifest_keys = list(manifest.keys())

        self.check_manifest(manifest_keys[0],
                            manifest,
                            [os.path.join(self.test_src, next_file) for next_file in self.test_trk_files_2014],
                            manifest[manifest_keys[0]]['start'],
                            end_dt)

    def check_manifest(self, manifest_key, manifest, file_list, start_dt, end_dt):
        '''Helper method used to check the results of a TRK manifest '''
        self.assertIn(build_trk_bundle.get_yydoy(start_dt), manifest_key)
        self.assertIn(build_trk_bundle.get_yydoy(end_dt), manifest_key)

        self.assertEqual(sorted(file_list),
                         sorted([entry['file_name'] for entry in manifest[manifest_key]['bundled_files']]))

        self.assertEqual(start_dt, manifest[manifest_key]['start'])
        self.assertEqual(end_dt, manifest[manifest_key]['end'])

