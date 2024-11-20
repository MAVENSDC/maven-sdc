'''
Created on Dec 16, 2015

@author: bstaley
'''
import unittest
import os
import pytz
from datetime import datetime, timedelta
from shutil import rmtree
from maven_utilities import constants
from maven_utilities.time_utilities import utc_now
os.environ[constants.python_env] = 'testing'
import re

from maven_database.models import ScienceFilesMetadata
from maven_public import utilities as maven_public_utils
from maven_data_file_indexer import utilities as maven_file_indexer_utils
from tests.maven_test_utilities import file_system, db_utils


class TestPublicScience(unittest.TestCase):

    def setUp(self):
        self.test_root = file_system.get_temp_root_dir()
        self.site_root = os.path.join(self.test_root, 'site')
        os.mkdir(self.site_root)
        self.data_root = os.path.join(self.test_root, 'data')
        self.sci_data_root = os.path.join(self.data_root, 'sci')

        self.sci_files = ['mvn_lpw_ql_20141016.png',
                          'mvn_mag_ql_20150707.png',
                          'mvn_ngi_ql_20150331.csv',
                          'mvn_pfp_l2_20150509.png',
                          'mvn_pfp_l2_20141011_000071.png',
                          'mvn_pfp_ql_20150714.png',
                          'mvn_sep_ql_20141005.png',
                          'mvn_sta_ql_20150214.png',
                          'mvn_swe_ql_20150629.png',
                          'mvn_swi_ql_20141012.png',
                          ]

        # Build science files
        file_system.build_test_files_and_structure(default_file_contents='something to fill the file',
                                                   files_base_dir=self.sci_data_root,
                                                   files_list=self.sci_files)

        # Add science files to metadata
        for sci_metadata in maven_file_indexer_utils.generate_metadata_for_ql_file([os.path.join(self.sci_data_root, f) for f in self.sci_files]):
            db_utils.insert_science_files_metadata(file_name=sci_metadata.file_name,
                                                   dir_path=sci_metadata.directory_path,
                                                   file_size=sci_metadata.file_size,
                                                   instrument=sci_metadata.instrument,
                                                   level=sci_metadata.level,
                                                   version=sci_metadata.version,
                                                   timetag=sci_metadata.timetag,
                                                   extension=sci_metadata.file_extension,
                                                   plan=sci_metadata.plan)

        self.science_files_metadata_truth = ScienceFilesMetadata.query.all()
        self.start_time = datetime(2014, 10, 1)
        self.end_time = datetime(2014, 12, 31)

    def tearDown(self):
        rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))
        db_utils.delete_data(ScienceFilesMetadata)

    def testBuildSiteSci(self):
        '''Tests building the public site'''
        maven_public_utils.build_site(root_dir=self.site_root,
                                      source_root_dir=self.data_root,
                                      sym_link=True)

        # assert all links exist.
        self.assertTrue(os.path.exists(os.readlink(os.path.join(self.site_root, maven_public_utils.SCI_DIR))))

        for next_public_file in self.sci_files:
            self.assertTrue(os.path.exists(os.readlink(os.path.join(self.site_root, maven_public_utils.SCI_DIR, next_public_file))))

    def testBuildSiteSymlink(self):
        '''Tests build_site that the sym links exist and are swapped or are created'''
        maven_public_utils.build_site(root_dir=self.test_root,
                                      source_root_dir=self.data_root,
                                      sym_link=True)

        # Assert that when sym links exist, they are swapped
        site_symlink_sci_dir = os.path.join(self.test_root, maven_public_utils.SCI_DIR)
        self.assertTrue(os.path.islink(site_symlink_sci_dir))
        if os.path.islink(site_symlink_sci_dir):
            os.unlink(site_symlink_sci_dir)
            self.assertFalse(os.path.islink(site_symlink_sci_dir))

    def testOrbitGetter(self):
        '''Test orbit_file_time_getter file regex patterns'''
        for file_name in self.sci_files:
            maven_public_utils.orbit_file_time_getter(file_name)
            file_regex = re.compile(maven_public_utils.orbit_file_pattern)
            self.assertIsNone(file_regex.match(os.path.basename(file_name)))

    def testSciQueryMetadataDates(self):
        '''Test SciQueryMetaData class for start_date, end_date, and version that is not None'''
        sqm_test = maven_public_utils.SciQueryMetadata(instrument_list=None,
                                                       plan_list=None,
                                                       file_extension_list=None,
                                                       start_date=self.start_time,
                                                       end_date=self.end_time,
                                                       version=1,
                                                       latest=False)

        timed_truth = [_next for _next in self.science_files_metadata_truth
                       if _next.timetag >= self.start_time and _next.timetag < self.end_time and _next.version == 1]

        timed_test = sqm_test.get_query()

        self.assertNotEqual(timed_truth, [], 'sci_query_metadata timed_truth is empty')
        for _next in timed_test.all():
            # Get the actual ScienceFileMetadata
            scfd = ScienceFilesMetadata.query.filter(ScienceFilesMetadata.file_name == _next[1]).first()
            self.assertGreaterEqual(scfd.timetag, self.start_time)
            self.assertLess(scfd.timetag, self.end_time)
            self.assertEqual(scfd.version, 1)

    def testSystemFileQueryMetadata(self):
        '''system_file_get_date method test for None and with no child_directories'''
        sqm_test = maven_public_utils.SystemFileQueryMetadata(root_dir=self.sci_data_root,
                                                              base_name_pattern='.*',
                                                              start_date=utc_now() - timedelta(days=1),
                                                              end_date=utc_now() + timedelta(days=1),
                                                              get_date=None,
                                                              child_directories=False)

        for _next in sqm_test.generate():
            self.assertIn(os.path.basename(_next), self.sci_files)
