"""
Created on Aug 22, 2016

@author: bstaley
"""

import unittest
import shutil
import os
import tarfile
import smtplib

try:
    from tests.make_pds_bundles import utilities as test_utilities
except ImportError:
    import utilities as test_utilities

from tests.maven_test_utilities import mail_utilities, log_handlers, file_system, db_utils
from maven_status import MAVEN_SDC_EVENTS
from maven_database.models import ScienceFilesMetadata, AncillaryFilesMetadata, PdsArchiveRecord, MavenStatus
from maven_ops_database.database import init_db
from make_pds_bundles import make_pds_bundles, config
from make_pds_bundles.results import GENERATION_SUCCESS
from maven_utilities.file_pattern import *
from maven_utilities import constants, maven_config
os.environ[constants.python_env] = 'testing'

# Setup in-memory database
init_db()


class TestInventoryGeneration(unittest.TestCase):
    '''
    '''

    # Remove ability to send emails
    smtplib.SMTP = mail_utilities.DummySMTP

    test_handler = log_handlers.RecordHandler()

    test_start = '2014-01-01'
    test_end = '2015-01-01'

    test_good_files = [
                       'mvn_iuv_l1a_outlimb-orbit00107-fuv_20141019T075533_v06_r00.fits',
                       'mvn_iuv_l1a_outlimb-orbit00107-fuv_20141018T075533_v04_r01.fits',
                       ]
    test_good_label_files = [
                             'mvn_iuv_l1a_outlimb-orbit00107-fuv_20141019T075533.xml',
                             'mvn_iuv_l1a_outlimb-orbit00107-fuv_20141018T075533.xml',
                             ]
    test_bad_label_files = ['mvn_iuv_l1a_outlimb-orbit00107-fuv_20161019T075533.xml']  # outside of test window
    test_out_of_window_files = ['mvn_iuv_l1a_outlimb-orbit00107-fuv_20141018T075533_v04_r00.fits', ]  # old revision
    test_bad_files = ['mvn_iuv_l1a_outlimb-orbit00107-fuv_20161019T075533_v06_r00.fits',  # outside of test window
                      'mvn_iuv_l1a_outlimb-orbit00108-fuv_20141019T075533_v06_r00.fits',  # not in inventory file
                      ]

    def setUp(self):
        self.test_root = file_system.get_temp_root_dir()
        self.test_root_maven = os.path.join(self.test_root, 'maven/data/sci')

        self.metadata_for_test_files = test_utilities.get_metadata(self.test_good_files + 
                                                                   self.test_good_label_files + 
                                                                   self.test_bad_label_files + 
                                                                   self.test_bad_files,
                                                                   self.test_root_maven)
        test_utilities.populate_science_metadata(next_data[0] for next_data in self.metadata_for_test_files)
        file_system.build_test_files_and_structure('some test data', self.test_root, [f[1] for f in self.metadata_for_test_files])

        self.inv_file_name = 'mvn_iuv_raw_collection_limb-inventory_20140711T224001.tab'
        self.inv_file_path = os.path.join(self.test_root_maven, 'iuv', 'metadata', self.inv_file_name)
        self.generate_pds_inventory_file(file_names=self.test_good_files + self.test_out_of_window_files,
                                         urn='maven.iuvs.raw',
                                         level='limb',
                                         file_name=self.inv_file_path)

    def tearDown(self):
        db_utils.delete_data(ScienceFilesMetadata, AncillaryFilesMetadata, PdsArchiveRecord, MavenStatus)
        shutil.rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))
        pass

    def test_inventory_pds_generation(self):
        # Hijack iuv PDS settings:
        config.instrument_config['iuv'] = config.ScienceFileSearchParameters(instrument='iuv',
                                                                             levels=[],
                                                                             plans=[],
                                                                             groups=[],
                                                                             descs=[],
                                                                             exts=[],
                                                                             file_name=None,
                                                                             ver=None,
                                                                             rev=None,
                                                                             as_inv_file=self.inv_file_path,
                                                                             uprev_inv_file=False,
                                                                             label_ver=1,
                                                                             label_rev=0)
        make_pds_bundles.run_archive(self.test_start, self.test_end, ['iuv'], self.test_root, False)
        results = PdsArchiveRecord.query.all()
        self.assertEqual(1, len(results))

        success_cnt = len([x for x in results if x.generation_result == GENERATION_SUCCESS])
        self.assertEqual(1, success_cnt)

        # Ensure files made it to archive.
        with tarfile.open(os.path.join(self.test_root, 'maven', 'data', 'arc', 'iuv', 'iuv-pds_bundle_2014-01-01-00-00-00_2015-01-01-00-00-00.tgz.1'),
                          'r:gz') as temp_tar:
            temp_file_names = temp_tar.getnames()
            temp_file_names = [os.path.basename(f) for f in temp_file_names]  # strip directory
            for _next in self.test_good_files:
                self.assertIn(_next, temp_file_names)
            for _next in self.test_good_label_files:
                self.assertIn(_next, temp_file_names)
            for _next in self.test_bad_files:
                self.assertNotIn(_next, temp_file_names)

    def test_missing_pds_file(self):

        # Generate an inventory file with an extra science file
        extra_sci_files = ['mvn_iuv_l1a_not-in-sdc_20141019T075533_v06_r00.fits', 'mvn_iuv_l1a_not-in-sdc2_20141019T075533_v06_r00.fits']
        self.generate_pds_inventory_file(file_names=self.test_good_files + extra_sci_files,
                                         urn='maven.iuvs.raw',
                                         level='limb',
                                         file_name=self.inv_file_path)
        config.instrument_config['iuv'] = config.ScienceFileSearchParameters(instrument='iuv',
                                                                             levels=[],
                                                                             plans=[],
                                                                             groups=[],
                                                                             descs=[],
                                                                             exts=[],
                                                                             file_name=None,
                                                                             ver=None,
                                                                             rev=None,
                                                                             as_inv_file=self.inv_file_path,
                                                                             uprev_inv_file=False,
                                                                             label_ver=1,
                                                                             label_rev=0)

        make_pds_bundles.run_archive(self.test_start, self.test_end, ['iuv'], self.test_root, False)

        maven_staus = list(MavenStatus.query.filter(MavenStatus.event_id == MAVEN_SDC_EVENTS.STATUS.name).all())

        self.assertEqual(len(extra_sci_files) * 2, len(maven_staus))  # *2 to include both label files as well as science files

        for _next_missing in extra_sci_files:
            pattern_to_check = re.sub(r'_v[\d].*$', '', _next_missing)
            for _next_status in maven_staus:
                if pattern_to_check in _next_status.summary:
                    break
            else:
                self.fail("%s wasn't found in any maven status" % pattern_to_check)

    def generate_pds_inventory_file(self,
                                    file_names,
                                    urn,
                                    level,
                                    file_name):


        if not os.path.isdir(os.path.dirname(file_name)):
            os.makedirs(os.path.dirname(file_name))

        with open(file_name, 'w') as inv_file:
            for file_name in file_names:
                m = extract_parts(regex_list=[maven_config.root_verrev_ext_regex],
                                  string_to_parse=file_name,
                                  parts=[general_root_group,
                                         general_version_group,
                                         general_revision_group])
                root, ver, rev = m.values()
                inv_file.write('P,urn:nasa:pds:{0}:{1}:{2}::{3}.{4}\n'.format(urn, level, root, int(ver), int(rev)))
