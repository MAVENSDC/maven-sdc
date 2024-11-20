'''
Created on Apr 28, 2016

@author: bstaley
'''
import os
import unittest
import smtplib
from shutil import rmtree
import mock

from maven_utilities import constants
from sqlalchemy.exc import DataError
os.environ[constants.python_env] = 'testing'
from tests.maven_test_utilities import file_system
from tests.maven_test_utilities.db_utils import delete_data
from tests.maven_test_utilities.mail_utilities import DummyMultiProcessSMTP
from maven_data_file_indexer import utilities


class DataFileIndexerUpsertTests(unittest.TestCase):
    # Hijack SMTP
    mail_capture = DummyMultiProcessSMTP
    smtplib.SMTP = mail_capture

    def setUp(self):
        self.test_root_dir = file_system.get_temp_root_dir()

        # Add some indexable files
        self.test_good_sci_files = ['mvn_sep_l2_s2-raw-svy-full_20160325_v04_r04.cdf',
                                    'mvn_sep_l2_s2-raw-svy-full_20160324_v04_r01.cdf',
                                    ]
        self.test_exception_sci_files = ['mvn_sep_l2_s2-raw-svy-full_20160311_v04_r02.cdf']

        self.test_good_anc_files = ['160401355SC202DSS35_noHdr.234',
                                    '160450045SC202DSS54_noHdr.234']

        self.test_exception_anc_files = ['151122145SC202DSS35_noHdr.234']

        self.test_good_l0_files = ['mvn_ins_grp_l0_20130430_v01.dat']

        self.test_exception_l0_files = ['mvn_ins_grp_l0_20130430_v03.dat']

        self.test_all_files = self.test_good_sci_files + \
            self.test_exception_sci_files +\
            self.test_good_anc_files + \
            self.test_exception_anc_files + \
            self.test_good_l0_files + \
            self.test_exception_l0_files

        file_system.build_test_files_and_structure(default_file_contents='some filler',
                                                   files_base_dir=self.test_root_dir,
                                                   files_list=self.test_all_files)

        self.sci_metadata = utilities.generate_metadata_for_science_file([os.path.join(self.test_root_dir, f) for f in self.test_good_sci_files + self.test_exception_sci_files])
        self.anc_metadata = utilities.generate_metadata_for_ancillary_file([os.path.join(self.test_root_dir, f) for f in self.test_good_anc_files + self.test_exception_anc_files])
        self.l0_metdata = utilities.generate_metadata_for_l0_file([os.path.join(self.test_root_dir, f) for f in self.test_good_l0_files + self.test_exception_l0_files])

    def tearDown(self):
        rmtree(self.test_root_dir)
        self.assertFalse(os.path.isdir(self.test_root_dir))
        delete_data()

    def mocked_science_insert(self, metadata):
        if metadata.file_name in self.test_exception_sci_files:
            raise DataError(1, 2, 3, 4)

    def mocked_l0_insert(self, metadata):
        if metadata.file_name in self.test_exception_l0_files:
            raise DataError(1, 2, 3, 4)

    def mocked_anc_insert(self, metadata):
        if metadata.file_name in self.test_exception_anc_files:
            raise DataError(1, 2, 3, 4)

    def test_upsert_raise(self):
        with mock.patch('maven_data_file_indexer.utilities.insert_science_file_metadatum',
                        side_effect=self.mocked_science_insert):
            with mock.patch('maven_data_file_indexer.utilities.insert_l0_file_metadatum',
                            side_effect=self.mocked_l0_insert):
                with mock.patch('maven_data_file_indexer.utilities.insert_ancillary_file_metadatum',
                                side_effect=self.mocked_anc_insert):

                    with self.assertRaises(Exception):
                        _ = utilities.upsert_science_file_metadata(self.sci_metadata)
                    with self.assertRaises(Exception):
                        _ = utilities.upsert_ancillary_file_metadata(self.anc_metadata)
                    with self.assertRaises(Exception):
                        _ = utilities.upsert_l0_file_metadata(self.l0_metdata)

    def test_upsert_exception_handling(self):
        with mock.patch('maven_data_file_indexer.utilities.insert_science_file_metadatum',
                        side_effect=self.mocked_science_insert):
            with mock.patch('maven_data_file_indexer.utilities.insert_l0_file_metadatum',
                            side_effect=self.mocked_l0_insert):
                with mock.patch('maven_data_file_indexer.utilities.insert_ancillary_file_metadatum',
                                side_effect=self.mocked_anc_insert):

                    sci_failed_metadata = utilities.upsert_science_file_metadata(self.sci_metadata, DataError)
                    anc_failed_metadata = utilities.upsert_ancillary_file_metadata(self.anc_metadata, DataError)
                    l0_failed_metadata = utilities.upsert_l0_file_metadata(self.l0_metdata, DataError)

                    for _next in sci_failed_metadata:
                        self.assertEqual(DataError, type(_next[0]))
                        self.assertIn(_next[1].file_name, self.test_exception_sci_files)
                    for _next in anc_failed_metadata:
                        self.assertEqual(DataError, type(_next[0]))
                        self.assertIn(_next[1].file_name, self.test_exception_anc_files)
                    for _next in l0_failed_metadata:
                        self.assertEqual(DataError, type(_next[0]))
                        self.assertIn(_next[1].file_name, self.test_exception_l0_files)
