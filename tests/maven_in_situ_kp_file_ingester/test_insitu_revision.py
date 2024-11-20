'''
Unit tests for the insitu ingest when multiple versions of kp files exist
(e.g earlier and later version based on the version/revision data.

Created on Mar 5, 2015

@author: bstaley
'''

import unittest
import os
import smtplib
from mock import patch, Mock
from shutil import rmtree
from maven_database.models import KpFilesMetadata
from maven_in_situ_kp_file_ingester import utilities, config
from maven_in_situ_kp_file_ingester.in_situ_kp_file_processor import insitu_file_processor
from tests.maven_test_utilities import file_system, db_utils, mail_utilities
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'


class TestInsituIngestRevision(unittest.TestCase):

    # Remove ability to send emails
    smtplib.SMTP = mail_utilities.DummySMTP

    def setUp(self):
        self.test_root = file_system.get_temp_root_dir()
        self.test_insitu_root = os.path.join(self.test_root, 'test_insitu_files')
        config.current_files_dir_name = os.path.join(self.test_root, 'current')
        config.failed_files_dir_name = os.path.join(self.test_root, 'failed')
        config.old_files_dir_name = os.path.join(self.test_root, 'old')
        config.updated_files_dir_name = os.path.join(self.test_root, 'updated')

        self.test_current_version = 0
        self.test_current_revision = 42
        self.test_current_filename = 'mvn_kp_insitu_20141017_v00_r42.tab'

        self.test_failed_version = 0
        self.test_failed_revision = 1
        self.test_failed_filename = 'mvn_kp_insitu_20141018_v00_r01.tab'

        self.test_new_filename = 'mvn_kp_insitu_20141019_v00_r01.tab'
        self.test_new_deprecated_filename = 'mvn_kp_insitu_20141019_v00_r00.tab'
        self.test_old_filename = 'mvn_kp_insitu_20141017_v00_r41.tab'
        self.test_updated_filename = 'mvn_kp_insitu_20141017_v00_r43.tab'
        # Add 'current' and 'failed' kp files
        m = db_utils.insert_kp_meta_data(self.test_current_filename,
                                         self.test_insitu_root,
                                         0,
                                         'in-situ',
                                         time_utilities.utc_now(),
                                         self.test_current_version,
                                         self.test_current_revision,
                                         'COMPLETE')

        m = db_utils.insert_kp_meta_data(self.test_failed_filename,
                                         self.test_insitu_root,
                                         0,
                                         'in-situ',
                                         time_utilities.utc_now(),
                                         self.test_failed_version,
                                         self.test_failed_revision,
                                         'STARTED')

        # Add test insitu kp files
        self.test_insitu_kp_files = [
            self.test_new_deprecated_filename,  # new deprecated
            self.test_new_filename,  # new
            self.test_old_filename,  # old
            self.test_current_filename,  # current
            self.test_updated_filename,  # updated
            self.test_failed_filename]  # failed

        file_system.build_test_files_and_structure("test kp data", self.test_insitu_root, self.test_insitu_kp_files)

    def tearDown(self):
        rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))
        db_utils.delete_data()

    def testFileStatusDetermination(self):
        file_status = utilities.get_insitu_kp_files_status(self.test_insitu_root)

        self.assertEqual(2, len(file_status.new))
        self.assertTrue(self.test_new_filename in [os.path.split(f[0])[1] for f in file_status.new])
        self.assertTrue(self.test_new_deprecated_filename in [os.path.split(f[0])[1] for f in file_status.new])
        self.assertEqual(1, len(file_status.deprecated))
        self.assertTrue(self.test_old_filename in file_status.deprecated[0][0])
        self.assertEqual(1, len(file_status.complete))
        self.assertTrue(self.test_current_filename in file_status.complete[0][0])
        self.assertEqual(1, len(file_status.updated))
        self.assertTrue(self.test_updated_filename in file_status.updated[0][0])
        self.assertEqual(1, len(file_status.started))
        self.assertTrue(self.test_failed_filename in file_status.started[0][0])

    def mock_insitu_ingest(self):
        pass

    def testFileStatusMove(self):
        with patch.object(insitu_file_processor, 'ingest_data', new=Mock(side_effect=self.mock_insitu_ingest)):
            utilities.ingest_in_situ_kp_data(self.test_insitu_root)
            # The only files left should be current and new
            remaining_files = os.listdir(self.test_insitu_root)
            self.assertEqual(6, len(remaining_files))

            # Assert the ingest statuses are what we expect
            kp_meta = KpFilesMetadata.query.filter(KpFilesMetadata.file_name == self.test_new_deprecated_filename).all()
            self.assertIsNotNone(kp_meta)
            self.assertEqual(1, len(kp_meta))
            self.assertEqual(config.kp_ingest_status_deprecated, kp_meta[0].ingest_status)

            kp_meta = KpFilesMetadata.query.filter(KpFilesMetadata.file_name == self.test_new_filename).all()
            self.assertIsNotNone(kp_meta)
            self.assertEqual(1, len(kp_meta))
            self.assertEqual(config.kp_ingest_status_started, kp_meta[0].ingest_status)

            kp_meta = KpFilesMetadata.query.filter(KpFilesMetadata.file_name == self.test_old_filename).all()
            self.assertIsNotNone(kp_meta)
            self.assertEqual(1, len(kp_meta))
            self.assertEqual(config.kp_ingest_status_deprecated, kp_meta[0].ingest_status)

            kp_meta = KpFilesMetadata.query.filter(KpFilesMetadata.file_name == self.test_current_filename).all()
            self.assertIsNotNone(kp_meta)
            self.assertEqual(1, len(kp_meta))
            self.assertEqual(config.kp_ingest_status_complete, kp_meta[0].ingest_status)

            kp_meta = KpFilesMetadata.query.filter(KpFilesMetadata.file_name == self.test_updated_filename).all()
            self.assertIsNotNone(kp_meta)
            self.assertEqual(1, len(kp_meta))
            self.assertEqual(config.kp_ingest_status_updated, kp_meta[0].ingest_status)

            kp_meta = KpFilesMetadata.query.filter(KpFilesMetadata.file_name == self.test_failed_filename).all()
            self.assertIsNotNone(kp_meta)
            self.assertEqual(1, len(kp_meta))
            self.assertEqual(config.kp_ingest_status_started, kp_meta[0].ingest_status)

            # Ensure re-ingest doesn't change anything
            utilities.ingest_in_situ_kp_data(self.test_insitu_root)

            kp_meta = KpFilesMetadata.query.filter(KpFilesMetadata.file_name == self.test_new_deprecated_filename).all()
            self.assertIsNotNone(kp_meta)
            self.assertEqual(1, len(kp_meta))
            self.assertEqual(config.kp_ingest_status_deprecated, kp_meta[0].ingest_status)

            kp_meta = KpFilesMetadata.query.filter(KpFilesMetadata.file_name == self.test_new_filename).all()
            self.assertIsNotNone(kp_meta)
            self.assertEqual(1, len(kp_meta))
            self.assertEqual(config.kp_ingest_status_started, kp_meta[0].ingest_status)

            kp_meta = KpFilesMetadata.query.filter(KpFilesMetadata.file_name == self.test_old_filename).all()
            self.assertIsNotNone(kp_meta)
            self.assertEqual(1, len(kp_meta))
            self.assertEqual(config.kp_ingest_status_deprecated, kp_meta[0].ingest_status)

            kp_meta = KpFilesMetadata.query.filter(KpFilesMetadata.file_name == self.test_current_filename).all()
            self.assertIsNotNone(kp_meta)
            self.assertEqual(1, len(kp_meta))
            self.assertEqual(config.kp_ingest_status_complete, kp_meta[0].ingest_status)

            kp_meta = KpFilesMetadata.query.filter(KpFilesMetadata.file_name == self.test_updated_filename).all()
            self.assertIsNotNone(kp_meta)
            self.assertEqual(1, len(kp_meta))
            self.assertEqual(config.kp_ingest_status_updated, kp_meta[0].ingest_status)

            kp_meta = KpFilesMetadata.query.filter(KpFilesMetadata.file_name == self.test_failed_filename).all()
            self.assertIsNotNone(kp_meta)
            self.assertEqual(1, len(kp_meta))
