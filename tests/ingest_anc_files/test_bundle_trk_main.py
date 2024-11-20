'''
Created on Jan 12, 2016

@author: cosc3564
'''
import unittest
import os
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from shutil import rmtree
import sys
from io import StringIO
import tarfile
from ingest_anc_files import build_trk_bundle, build_trk_bundle_main
from tests.maven_test_utilities import file_system, db_utils


class TestBuildTrkBundleMain(unittest.TestCase):
    doys = ['001', '002', '003', '004', '005', '006', '007']

    def setUp(self):
        self.root_dir_temp = file_system.get_temp_root_dir()
        self.tracking_data = os.path.join(self.root_dir_temp, 'tracking_data_trk')
        os.mkdir(self.tracking_data)
        self.output_dir_temp = os.path.join(self.root_dir_temp, 'output_trk')
        os.mkdir(self.output_dir_temp)

        build_trk_bundle.shelf_file = self.root_dir_temp + '/.trk_bundle_manifest'
        sys.argv = ['TestBuildTrkBundleMain', '--start-date', '2015-01-02', '--end-date',
                    '2015-01-06', '-t', 'latest', '--print-manifest',
                    '--output-dir', self.output_dir_temp]

        for next_doy in self.doys:
            next_file = '15{0}1740SC202DSS14_noHdr.234'.format(next_doy)
            db_utils.insert_ancillary_file_metadata(file_name=next_file,
                                                    file_extension='234',
                                                    start_date=build_trk_bundle.get_dt_from_yydoy(2015, int(next_doy)),
                                                    directory_path=self.tracking_data)
            with open(os.path.join(self.tracking_data, next_file), 'wb') as f:
                f.write(next_file.encode())

    def tearDown(self):
        rmtree(self.root_dir_temp)
        self.assertFalse(os.path.isdir(self.root_dir_temp))
        db_utils.delete_data()

    def testStartAndEndDatePass(self):
        sys.argv = ['TestBuildTrkBundleMain', '--start-date', '2015-01-02', '--end-date',
                    '2015-01-06', '--output-dir',
                    self.output_dir_temp]
        build_trk_bundle_main.main()
        temp_tarfile_dir = os.path.join(self.output_dir_temp, 'mvn_anc_trk_15002_15006.tgz')
        self.assertTrue(tarfile.is_tarfile(temp_tarfile_dir), 'temp_file is not a tarfile')

        with tarfile.open(temp_tarfile_dir, 'r:gz') as temp_file:
            temp_file_names = temp_file.getnames()
            self.assertNotEqual(temp_file_names, [], 'temp_file is empty, no members')
            given_date_range = ['002', '003', '004', '005']
            for date in ['15{0}1740SC202DSS14_noHdr.234'.format(i) for i in given_date_range]:
                self.assertIn(date, [os.path.basename(fn) for fn in temp_file_names])
            for date in ['15{0}1740SC202DSS14_noHdr.234'.format(i) for i in set(self.doys) - set(given_date_range)]:
                self.assertNotIn(date, [os.path.basename(fn) for fn in temp_file_names])

    def testLatest(self):
        sys.argv = ['TestBuildTrkBundleMain', '-t', 'latest', '-i', '10000',
                    '--output-dir', self.output_dir_temp]
        build_trk_bundle_main.main()
        output_bundle = [f for f in os.listdir(self.output_dir_temp) if 'mvn_anc_trk' in f]
        with tarfile.open(os.path.join(self.output_dir_temp, output_bundle[0]), 'r:gz') as temp_file:
            temp_file_names = temp_file.getnames()
            self.assertNotEqual(temp_file_names, [], 'temp_file is empty, no members')
            compare_temp_files = [os.path.basename(fn) for fn in temp_file_names]
            for date in ['15{0}1740SC202DSS14_noHdr.234'.format(i) for i in self.doys]:
                self.assertIn(date, [os.path.basename(fn) for fn in compare_temp_files])

    def testPrintManifest(self):
        sys.argv = ['TestBuildTrkBundleMain', '-t', 'latest',
                    '--output-dir', self.output_dir_temp]
        build_trk_bundle_main.main()
        sys.argv = ['TestBuildTrkBundleMain', '--print-manifest', '--output-dir', self.output_dir_temp]
        sys.stdout = manifest_output = StringIO()
        build_trk_bundle_main.main()
        for date in ['15{0}1740SC202DSS14_noHdr.234'.format(i) for i in self.doys]:
            self.assertNotEqual(manifest_output, [], 'manifest is empty')
            self.assertIn(date, manifest_output.getvalue())
    
    def testBundleTypeFull(self):
        sys.argv = ['TestBuildTrkBundleMain', '-t', 'full',
                    '--output-dir', self.output_dir_temp]
        build_trk_bundle_main.main()
        output_bundle = [f for f in os.listdir(self.output_dir_temp) if 'mvn_anc_trk' in f]
        non_empty_temp_file = []
        for output_file in output_bundle:
            with tarfile.open(os.path.join(self.output_dir_temp, output_file), 'r:gz') as temp_file:
                temp_file_name = temp_file.getnames()
                if temp_file_name != []:
                    non_empty_temp_file.append(temp_file_name[0])
        self.assertNotEqual(non_empty_temp_file, [], 'temp_file is empty, no members')
    
    def testBundleTypeIncremental(self):
        sys.argv = ['TestBuildTrkBundleMain', '-t', 'incremental',
                    '--output-dir', self.output_dir_temp]
        build_trk_bundle_main.main()
        output_bundle = [f for f in os.listdir(self.output_dir_temp) if 'mvn_anc_trk' in f]
        non_empty_temp_file = []
        for output_file in output_bundle:
            with tarfile.open(os.path.join(self.output_dir_temp, output_file), 'r:gz') as temp_file:
                non_empty_temp_file.append(temp_file)
        self.assertNotEqual(non_empty_temp_file, [], 'temp_file is empty, no members')