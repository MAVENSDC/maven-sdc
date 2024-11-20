'''
Created on Dec 7, 2015

@author: bstaley
'''
import unittest
import datetime
import os
from shutil import rmtree
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from maven_database import db_session
from maven_database.models import ScienceFilesMetadata, PdsArchiveRecord
from maven_public import utilities as maven_public_utils
from tests.maven_test_utilities import file_system, db_utils
from maven_utilities import time_utilities


class TestMavenPublic(unittest.TestCase):

    def setUp(self):
        # Populate science files
        # Populate metadata
        self.test_root = file_system.get_temp_root_dir()
        self.site_root = os.path.join(self.test_root, 'site')
        os.mkdir(self.site_root)
        self.data_root = os.path.join(self.test_root, 'data')
        self.public_releases_root = os.path.join(self.data_root, 'sdc/public_releases')
        os.makedirs(self.public_releases_root)
        self.sci_data_root = os.path.join(self.data_root, 'sci')
        self.checksum_file = 'public_test_checksum.txt'
        # generate some random public science files

        for i in range(100):
            db_utils.insert_science_files_metadata(grouping='public-test-file-%s' % i,
                                                   dir_path=self.sci_data_root,
                                                   timetag=datetime.datetime(2015,1,1),
                                                   instrument='iuv',
                                                   extension='png')

        # Some are public, some are not
        self.science_files = [(record.file_name, True if record.id % 2 else False) for record in ScienceFilesMetadata.query.all()]

        file_system.build_test_files_and_structure(default_file_contents='something to fill the file',
                                                   files_base_dir=self.sci_data_root,
                                                   files_list=[x[0] for x in self.science_files])

        # Generate a checksum for public files.
        file_system.build_test_files_and_structure(default_file_contents='\n'.join(['{}  {}'.format('SOMEBOGUSCHECKSUM', os.path.join(self.sci_data_root, next_file[0])) for next_file in self.science_files if next_file[1]]),
                                                   files_base_dir=self.data_root,
                                                   files_list=[self.checksum_file]
                                                   )

        test_pds_archive_record = PdsArchiveRecord(generation_time=time_utilities.utc_now(),
                                                   start_time=time_utilities.utc_now(),
                                                   end_time=time_utilities.utc_now(),
                                                   command_line='bogus cmd line',
                                                   configuration='bogus config',
                                                   dry_run=False,
                                                   result_directory=self.data_root,
                                                   bundle_file_name='dont care',
                                                   manifest_file_name='dont care',
                                                   checksum_file_name=self.checksum_file,
                                                   result_version=42,
                                                   generation_result=maven_public_utils.PDS_SUCCESS_FLAG,
                                                   pds_status=maven_public_utils.PDS_RELEASE_FLAG)

        db_session.add(test_pds_archive_record)
        db_session.commit()

    def tearDown(self):
        rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))
        db_utils.delete_data(ScienceFilesMetadata, PdsArchiveRecord)

    def test_populate_links(self):
        maven_public_utils.build_site(self.site_root, self.data_root, True)

        # assert all links exist.
        self.assertTrue(os.path.exists(os.readlink(os.path.join(self.site_root, maven_public_utils.SCI_DIR))))

        for next_public_file in [next_file[0] for next_file in self.science_files]:
            self.assertTrue(os.path.exists(os.readlink(os.path.join(self.site_root, maven_public_utils.SCI_DIR, next_public_file))))
