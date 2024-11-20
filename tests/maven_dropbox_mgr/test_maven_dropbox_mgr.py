import os
import shutil
from shutil import rmtree
import unittest
import hashlib
import gzip
import csv
from maven_utilities import constants, anc_config
os.environ[constants.python_env] = 'testing'
from maven_dropbox_mgr import utilities, config
from maven_utilities import time_utilities, constants
from maven_database.models import MavenDropboxMgrMove, MavenLog
from mock import Mock
from tests.maven_test_utilities.file_system import get_temp_root_dir
from tests.maven_test_utilities.db_utils import delete_data
from tests.maven_test_utilities import mail_utilities
from maven_utilities import mail, maven_log, maven_config, file_pattern
import smtplib


class MavenDropboxMgrTestCase(unittest.TestCase):
    '''Tests of the maven dropbox manager.'''

    smtplib.SMTP = mail_utilities.DummySMTP

    def setUp(self):
        self.test_root = get_temp_root_dir()
        config.root_destination_directory = os.path.join(
            self.test_root, 'dest')
        os.mkdir(config.root_destination_directory)
        assert os.path.isdir(config.root_destination_directory)

        config.pocdrop_destination_directory = config.root_destination_directory
        self.root_source_directory = os.path.join(self.test_root, 'src')
        os.mkdir(self.root_source_directory)
        assert os.path.isdir(self.root_source_directory)

        self.invalid_dir = os.path.join(
            self.root_source_directory, config.invalid_dir_name)
        self.dupe_dir = os.path.join(
            self.root_source_directory, config.dupe_dir_name)
        maven_log.config_logging()

        constants.filename_transforms_location = os.path.join(
            self.test_root, 'mavenpro', 'filename_transforms.csv')
        os.mkdir(os.path.join(self.test_root, 'mavenpro'))
        
        os.mkdir(self.dupe_dir)
        os.mkdir(self.invalid_dir)

    def tearDown(self):
        rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))
        delete_data(MavenDropboxMgrMove, MavenLog)

    def smoke_test(self):
        self.assertTrue(True)

    def get_abs_filename(self, dropbox_file_name, build_parent=True):
        '''Method used to create the dropbox destination directory if that directory doesn't exist'''
        abs_filename = None

        for next_pattern, groups_to_check, _, rename_rule, _, _, _ in config.file_rules:
            m = file_pattern.matches_on_group(
                [next_pattern], dropbox_file_name, groups_to_check)
            if m is not None:
                base_dir, dynamic_dir = rename_rule(m)
                abs_dir = base_dir
                if dynamic_dir is not None:
                    abs_dir = os.path.join(base_dir, dynamic_dir)
                if not os.path.isdir(abs_dir) and build_parent:
                    os.makedirs(abs_dir)
                abs_filename = os.path.join(abs_dir, dropbox_file_name)
                return abs_filename
        return abs_filename

    def test_logging_to_database(self):
        test_message = 'test logging_to_database message'
        count = MavenLog.query.count()
        self.assertEqual(count, 0)
        utilities.db_logger.info(test_message)
        count = MavenLog.query.count()
        self.assertEqual(count, 1)
        log = MavenLog.query.first()
        self.assertEqual(log.logger, 'maven.maven_dropbox_mgr.utilities.db_log')
        self.assertEqual(log.level, 'INFO')
        self.assertIn(test_message, log.message)
        t = time_utilities.utc_now().replace(tzinfo=None)  # sqlite can't store timezone info
        tdiff = t - log.created_at
        self.assertTrue(tdiff.seconds < 5)

    def test_is_valid_dropbox_file(self):
        # Test general File
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_l1_descriptor_20130618T010203_v01_r01.png'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_l1_descriptor_20130618T010203_v01_r01.fits'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_l1_descriptor_20130618T010203_v01_r01.cdf'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_l1_descriptor_20130618T010203_v01_r01.txt'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_l1_descriptor_20130618T010203_v01_r01.csv'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_l1_descriptor_20130618T010203_v01_r01.md5'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_l1_descriptor_20130618T010203_v01_r01.tab'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_l1_descriptor_20130618T010203_v01_r01.sav'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_l1_descriptor_20130618T010203_v01_r01.png.gz'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_l1_descriptor_20130618T010203_v01_r01.fits.gz'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_l1_descriptor_20130618T010203_v01_r01.cdf.gz'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_l1_descriptor_20130618T010203_v01_r01.txt.gz'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_l1_descriptor_20130618T010203_v01_r01.csv.gz'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_l1_descriptor_20130618T010203_v01_r01.md5.gz'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_l1_descriptor_20130618T010203_v01_r01.tab.gz'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_l1_descriptor_20130618T010203_v01_r01.tab.gz'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_iuv_l1a_fuv-ISON1-wide-cycle03_20131212T090324_v00_r00.fits.gz'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_euv_l2b_orbit_merged_v08_r01.sav'))
        self.assertFalse(utilities.is_valid_dropbox_file('totally_bogus_file'))
        self.assertFalse(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_descriptor_20130618T010203_v01_r01.png'))  # no level

        # Test Quicklook
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_20070516.png'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_20070516.csv'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_descriptor_20130618T010203_v01_r01.png'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_descriptor_20130618T010203_v01_r01.fits'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_descriptor_20130618T010203_v01_r01.cdf'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_descriptor_20130618T010203_v01_r01.txt'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_descriptor_20130618T010203_v01_r01.csv'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_descriptor_20130618T010203_v01_r01.md5'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_2014d197pl_20140716_v00_r02.sts'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_descriptor_20130618T010203_v01_r01.png.gz'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_descriptor_20130618T010203_v01_r01.fits.gz'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_descriptor_20130618T010203_v01_r01.cdf.gz'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_descriptor_20130618T010203_v01_r01.txt.gz'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_descriptor_20130618T010203_v01_r01.csv.gz'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_descriptor_20130618T010203_v01_r01.md5.gz'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_iuv_ql_reflectance-orbit00335-muv_20141201T015734_v04_r01.jpg'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_pfp_l2_20150627_001445.png'))

        self.assertFalse(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/bws_test_ql_20070516.png'))  # doesn't start with mvn
        self.assertFalse(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_20070516.bws'))  # bad extension
        self.assertFalse(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/not-a-mvn_test_ql_descriptor_20130618T010203_v01_r01.png'))
        self.assertFalse(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_descriptor_20130618T010203_r01.png'))  # no version
        self.assertFalse(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_test_ql_descriptor_20130618T010203_v01.png'))  # no revision
        self.assertFalse(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_ql_descriptor_20130618T010203_v01_r01.png'))  # no instrument

        # Test In-situ KP File
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_kp_insitu_20141221_v01_r01.tab'))

        # Test IUVS KP File
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_kp_iuvs_20141229T201950_v00_r00.tab'))

        self.assertFalse(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_kp_insitu_20141221_r01.tab'))  # no version
        self.assertFalse(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_kp_insitu_20141221_v01.tab'))  # no revision

        # Test SEP Anc File
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_sep_anc_20141221_v01_r01.sav'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_sep_anc_20141221T000000_v01_r01.sav'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_sep_anc_20141221_v01_r01.cdf'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_sep_l2_anc_20141221_v01_r01.cdf'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_sep_l2_pad_20160101_v01_r01.cdf'))
        
        # Test Label Files
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_ngi_l2_csn-abund-15090_20150226t141355_v03_r01.xml'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_ngi_l2_csn-abund-15090_20150226t141355_v03_r01.xml.gz'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_ngi_l2_csn-abund-15090_20150226t141355.xml.gz'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvn_kp_iuvs_20141021T223510.xml'))

        # Test metadata files
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/collection_maven_iuv_processed_schema.xml'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/bundle_maven_iuv_processed.xml'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/collection_ngi_context.csv'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/ngims_tid_caveats_v01_r02.pdf'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/ngims_tid_caveats_v01_r02.xml'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/ngims_tid_caveats.pdf'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/ngims_readme.txt'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/iuvs_data_readme.txt'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/ngims_product_version_changes_v02_r01.pdf'))

        # Test Ancillary Radio Files
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvnmaoi2016057_2000X35X35RM.1B1.resid.txt'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvnmaoc20170012017151_60.wea'))
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvnmaoc2017_055_1921x26mv1.tnf'))
        self.assertFalse(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvnmaoc2017055_2316x26x26rd.3b5'))  # Extension is not valid
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/mvnmaoc2017055_2316x26x26rd.2a1'))
        # Verify no dependency on upper/lower case
        self.assertTrue(utilities.is_valid_dropbox_file(
            '/path/to/nowhere/202MAOE2018208_1832X42X36RM.1A1.RESID.TXT'))

    def test_move_kp_insitu_file(self):
        utcnow = time_utilities.utc_now().replace(tzinfo=None)  # sqlite can't store timezone info

        try:
            bn = 'mvn_kp_insitu_20141231_v01_r01.tab'
            dest_fn = self.get_abs_filename(bn, True)
            src_fn = os.path.join(self.root_source_directory, bn)
            with open(src_fn, 'w') as f:
                f.write('just something to fill the file')
            before_db_count = MavenDropboxMgrMove.query.count()

            utilities.move_valid_dropbox_file(src_fn,
                                              self.invalid_dir,
                                              self.dupe_dir)
            # it disappeared from the source directory
            self.assertFalse(os.path.exists(src_fn))
            # and appeared in the destination directory
            self.assertTrue(os.path.isfile(dest_fn))
            after_db_count = MavenDropboxMgrMove.query.count()
            self.assertEqual(after_db_count, before_db_count + 1)
            m = MavenDropboxMgrMove.query.first()
            tdiff = m.when_moved - utcnow
            self.assertTrue(tdiff.seconds < 10)
            self.assertEqual(m.src_filename, src_fn)
            self.assertEqual(m.dest_filename, dest_fn)
            self.assertEqual(
                m.md5, hashlib.md5(open(dest_fn).read().encode()).hexdigest())
            self.assertEqual(m.file_size, os.path.getsize(dest_fn))
        finally:
            shutil.rmtree(config.pocdrop_destination_directory)


    def test_flares_dailyplots(self):
        utcnow = time_utilities.utc_now().replace(tzinfo=None)  # sqlite can't store timezone info
        bn = 'mvn_euv_20250101_0000.png'
        dest_fn = self.get_abs_filename(bn, True)
        src_fn = os.path.join(self.root_source_directory, bn)
        with open(src_fn, 'w') as f:
            f.write('just something to fill the file')
        #before_db_count = MavenDropboxMgrMove.query.count()
        utilities.move_valid_dropbox_file(src_fn,
                                            self.invalid_dir,
                                            self.dupe_dir)
        # it disappeared from the source directory
        self.assertFalse(os.path.exists(src_fn))
        # and appeared in the destination directory
        self.assertTrue(os.path.isfile(dest_fn))

    def test_flares_plots(self):
        utcnow = time_utilities.utc_now().replace(tzinfo=None)  # sqlite can't store timezone info
        bn = "mvn_euv_flare_20240503_0341_C5.1.png"
        dest_fn = self.get_abs_filename(bn, True)
        src_fn = os.path.join(self.root_source_directory, bn)
        with open(src_fn, 'w') as f:
            f.write('just something to fill the file')
        #before_db_count = MavenDropboxMgrMove.query.count()
        utilities.move_valid_dropbox_file(src_fn,
                                            self.invalid_dir,
                                            self.dupe_dir)
        # it disappeared from the source directory
        self.assertFalse(os.path.exists(src_fn))
        # and appeared in the destination directory
        self.assertTrue(os.path.isfile(dest_fn))


    def test_duplicate_ignore(self):
        utcnow = time_utilities.utc_now().replace(tzinfo=None)  # sqlite can't store timezone info

        try:
            bn = 'mvn_kp_insitu_20141231_v01_r01.tab'
            dest_fn = self.get_abs_filename(bn, True)
            src_fn = os.path.join(self.root_source_directory, bn)
            with open(src_fn, 'w') as f:
                f.write('just something to fill the file')
            before_db_count = MavenDropboxMgrMove.query.count()

            utilities.move_valid_dropbox_file(src_fn,
                                              self.invalid_dir,
                                              self.dupe_dir)
            # it disappeared from the source directory
            self.assertFalse(os.path.exists(src_fn))
            # and appeared in the destination directory
            self.assertTrue(os.path.isfile(dest_fn))
            after_db_count = MavenDropboxMgrMove.query.count()
            self.assertEqual(after_db_count, before_db_count + 1)
            m = MavenDropboxMgrMove.query.first()
            tdiff = m.when_moved - utcnow
            self.assertTrue(tdiff.seconds < 10)
            self.assertEqual(m.src_filename, src_fn)
            self.assertEqual(m.dest_filename, dest_fn)
            self.assertEqual(
                m.md5, hashlib.md5(open(dest_fn).read().encode()).hexdigest())
            self.assertEqual(m.file_size, os.path.getsize(dest_fn))
        finally:
            shutil.rmtree(config.pocdrop_destination_directory)
        
    def test_loc_generation(self):
        result = config.general_absolute_loc_generation(maven_config.science_regex.match(
            'mvn_test_l1_descriptor_20130618T010203_v01_r01.png'.lower()))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        self.assertIn('2013', result[1].split('/'))
        self.assertIn('06', result[1].split('/'))

        result = config.playback_absolute_loc_generation(
            maven_config.playback_file_regex.match('pfp_test_playback.test2.42'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))

        result = config.ql_absolute_loc_generation(
            maven_config.ql_regex.match('mvn_test_ql_20070516.png'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        self.assertIn('test', result[0].split('/'))
        self.assertIn('ql', result[0].split('/'))

        result = config.sep_anc_absolute_loc_generation(
            maven_config.sep_anc_regex.match('mvn_sep_l2_anc_20141221_v01_r01.sav'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        self.assertIn('2014', result[1].split('/'))
        self.assertIn('12', result[1].split('/'))
        
        result = config.sep_anc_absolute_loc_generation(
            maven_config.sep_anc_regex.match('mvn_sep_l2_pad_20141221_v01_r01.sav'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        self.assertIn('2014', result[1].split('/'))
        self.assertIn('12', result[1].split('/'))
        self.assertIn('pad', result[0].split('/'))
        
        result = config.sweal3_loc_generation(
            maven_config.science_regex.match('mvn_swe_l3_padscore_20160101_v08_r01.sav'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results1 = result[1].split('/')
        results0 = result[0].split('/')
        self.assertIn('padscore', results0)
        self.assertIn('2016', results1)
        self.assertIn('01', results1)
        
        result = config.sweal3_loc_generation(
            maven_config.science_regex.match('mvn_swe_l3_shape_20150501_v01_r01.sav'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results1 = result[1].split('/')
        results0 = result[0].split('/')
        self.assertIn('shape', results0)
        self.assertIn('2015', results1)
        self.assertIn('05', results1)
        
        result = config.euv_l2b_absolute_loc_generation(
            maven_config.euv_l2b_regex.match('mvn_euv_l2b_orbit_merged_v08_r01.sav'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        self.assertIsNone(result[1])
        results1 = result[0].split('/')
        self.assertIn('l2b', results1)

        result = config.kp_absolute_loc_generation(
            maven_config.kp_regex.match('mvn_kp_insitu_20141221_v01_r01.tab'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        self.assertIn('2014', result[1].split('/'))
        self.assertIn('12', result[1].split('/'))

        result = config.kp_absolute_loc_generation(
            maven_config.kp_regex.match('mvn_kp_iuvs_20141229T201950_v00_r00.tab'.lower()))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        self.assertIn('2014', result[1].split('/'))
        self.assertIn('12', result[1].split('/'))

        result = config.label_file_absolute_loc_generation(maven_config.label_regex.match(
            'mvn_ngi_l2_csn-abund-15090_20150226t141355_v03_r01.xml'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        self.assertIn('2015', result[1].split('/'))
        self.assertIn('02', result[1].split('/'))

        result = config.iuvs_loc_generation(maven_config.science_regex.match(
            'mvn_iuv_l1b_outbound-orbit00502-ech_20150101T223618_v02_r00.fits.gz'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results1 = result[1].split('/')
        results0 = result[0].split('/')
        self.assertIn('echelle', results0)
        self.assertIn('2015', results1)
        self.assertIn('01', results1)

        result = config.iuvs_loc_generation(maven_config.science_regex.match(
            'mvn_iuv_l1a_periapse-orbit00502-fuv_20150101T214711_v02_r00.fits'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results1 = result[1].split('/')
        results0 = result[0].split('/')
        self.assertIn('limb', results0)
        self.assertIn('2015', results1)
        self.assertIn('01', results1)

        result = config.iuvs_loc_generation(maven_config.science_regex.match(
            'mvn_iuv_l1b_apoapse-orbit00502-fuv_20150101T231438_v02_r01.fits.gz'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results1 = result[1].split('/')
        results0 = result[0].split('/')
        self.assertIn('disk', results0)
        self.assertIn('2015', results1)
        self.assertIn('01', results1)

        result = config.iuvs_loc_generation(maven_config.science_regex.match(
            'mvn_iuv_l1a_phobos-orbit02165-muv_20151110T021108_v04_r01.fits'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results1 = result[1].split('/')
        results0 = result[0].split('/')
        self.assertIn('phobos', results0)
        self.assertIn('2015', results1)
        self.assertIn('11', results1)

        result = config.iuvs_loc_generation(maven_config.science_regex.match(
            'mvn_iuv_ql_o3-orbit01341-muv_20150608T142731_v04_r01.jpg'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results1 = result[1].split('/')
        results0 = result[0].split('/')
        self.assertIn('disk', results0)
        self.assertIn('2015', results1)
        self.assertIn('06', results1)

        result = config.iuvs_loc_generation(maven_config.science_regex.match(
            'mvn_iuv_ql_reflectance-orbit01600-muv_20150726T074755_v04_r01.jpg'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results1 = result[1].split('/')
        results0 = result[0].split('/')
        self.assertIn('disk', results0)
        self.assertIn('2015', results1)
        self.assertIn('07', results1)\
        
        result = config.iuvs_loc_generation(maven_config.science_regex.match(
            'mvn_iuv_l1a_indisk-orbit03442-fuv_20160706T061156_v07_r01.fits'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results1 = result[1].split('/')
        results0 = result[0].split('/')
        self.assertIn('disk', results0)
        self.assertIn('2016', results1)
        self.assertIn('07', results1)

        result = config.iuvs_loc_generation(maven_config.science_regex.match(
            'mvn_iuv_l1b_inspace-orbit00117-fuv_20141020T083012_v07_r01.fits'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results1 = result[1].split('/')
        results0 = result[0].split('/')
        self.assertIn('corona', results0)
        self.assertIn('2014', results1)
        self.assertIn('10', results1)
        
        result = config.iuvs_loc_generation(maven_config.science_regex.match(
            'mvn_iuv_l1a_inlimb-orbit036420-muv_20160811T195027_v07_r01.fits'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results1 = result[1].split('/')
        results0 = result[0].split('/')
        self.assertIn('limb', results0)
        self.assertIn('2016', results1)
        self.assertIn('08', results1)

        result = config.ql_absolute_loc_generation(
            maven_config.ql_regex.match('mvn_pfp_l2_20150627_001445.png'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results1 = result[1].split('/')
        results0 = result[0].split('/')
        self.assertIn('pfp', results0)
        self.assertIn('l2', results0)
        self.assertIn('single_orbit', results1)

        result = config.metadata_absolute_loc_generation(
            maven_config.metadata_regex.match('collection_maven_iuv_processed_schema.xml'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results1 = result[1].split('/')
        results0 = result[0].split('/')
        self.assertIn('iuv', results0)
        self.assertIn(config.metadata_dir_name, results1)

        result = config.metadata_absolute_loc_generation(
            maven_config.metadata_regex.match('collection_ngi_context.csv'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results1 = result[1].split('/')
        results0 = result[0].split('/')
        self.assertIn('ngi', results0)
        self.assertIn(config.metadata_dir_name, results1)

        result = config.metadata_absolute_loc_generation(
            maven_config.metadata_regex.match('bundle_maven_ngims.xml'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results1 = result[1].split('/')
        results0 = result[0].split('/')
        self.assertIn('ngi', results0)
        self.assertIn(config.metadata_dir_name, results1)

        result = config.metadata_absolute_loc_generation(
            maven_config.metadata_caveats_regex.match('ngims_tid_caveats_v01_r02.pdf.gz'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results1 = result[1].split('/')
        results0 = result[0].split('/')
        self.assertIn('ngi', results0)
        self.assertIn(config.metadata_dir_name, results1)

        result = config.radio_absolute_loc_generation(
            anc_config.radio_resid_regex.match('mvnmaoi2016229_1733x65x65rd.1b1.resid.txt'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results0 = result[0].split('/')
        results1 = result[1].split('/')
        self.assertIn('anc', results0)
        self.assertIn('l2a', results1)
        
        result = config.radio_absolute_loc_generation(
            anc_config.radio_l3a_regex.match('mvnmaoc20170012017151_60.wea'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results0 = result[0].split('/')
        results1 = result[1].split('/')
        self.assertIn('anc', results0)
        self.assertIn('l2a', results1)
        
        result = config.radio_absolute_loc_generation(
            anc_config.radio_data_regex.match('mvnmaoc2017_055_1921x26mv1.tnf'))
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        results0 = result[0].split('/')
        results1 = result[1].split('/')
        self.assertIn('anc', results0)
        self.assertIn('l0a', results1)

    def test_move_playback_file(self):
        utcnow = time_utilities.utc_now().replace(tzinfo=None)  # sqlite can't store timezone info

        try:
            if not os.path.exists(config.pocdrop_destination_directory):
                os.makedirs(config.pocdrop_destination_directory)
            assert os.path.isdir(config.pocdrop_destination_directory)
            bn = 'pfp_test_playback.test2.42'
            src_fn = os.path.join(self.root_source_directory, bn)
            with open(src_fn, 'w') as f:
                f.write('just something to fill the file')
            before_db_count = MavenDropboxMgrMove.query.count()

            utilities.move_valid_dropbox_file(src_fn,
                                              self.invalid_dir,
                                              self.dupe_dir)
            # it disappeared from the source directory
            self.assertFalse(os.path.exists(src_fn))
            dest_fn = os.path.join(config.pocdrop_destination_directory, bn)
            # and appeared in the destination directory
            self.assertTrue(os.path.isfile(dest_fn))
            after_db_count = MavenDropboxMgrMove.query.count()
            self.assertEqual(after_db_count, before_db_count + 1)
            m = MavenDropboxMgrMove.query.first()
            tdiff = m.when_moved - utcnow
            self.assertTrue(tdiff.seconds < 10)
            self.assertEqual(m.src_filename, src_fn)
            self.assertEqual(m.dest_filename, dest_fn)
            self.assertEqual(
                m.md5, hashlib.md5(open(dest_fn).read().encode()).hexdigest())
            self.assertEqual(m.file_size, os.path.getsize(dest_fn))
        finally:
            shutil.rmtree(config.pocdrop_destination_directory)

    def test_move_zip_file(self):
        utcnow = time_utilities.utc_now().replace(tzinfo=None)  # sqlite can't store timezone info

        formatted_time = utcnow.strftime('%Y%m%dT%H%M%S')
        year_str = utcnow.strftime('%Y')
        month_str = utcnow.strftime('%m')
        dest_dir = os.path.join(
            config.root_destination_directory, 'test', 'ql')

        os.makedirs(dest_dir)
        assert os.path.isdir(dest_dir)
        bn = 'mvn_test_ql_descriptor_' + formatted_time + '_v02_r01.png'
        src_fn = os.path.join(self.root_source_directory, bn)
        with open(src_fn, 'w') as f:
            f.write('just something to fill the file')

        gzip_file_name = src_fn + ".gz"
        # zip file
        with gzip.GzipFile(gzip_file_name, 'w') as gzipfile:
            gzipfile.write(src_fn.encode())
        # remove src_fn
        os.remove(src_fn)

        before_db_count = MavenDropboxMgrMove.query.count()

        utilities.move_valid_dropbox_file(gzip_file_name,
                                          self.invalid_dir,
                                          self.dupe_dir)
        # it disappeared from the source directory
        self.assertFalse(os.path.exists(gzip_file_name))
        dest_fn = os.path.join(dest_dir, year_str, month_str, bn)
        # and appeared in the destination directory
        self.assertTrue(os.path.isfile(dest_fn))
        after_db_count = MavenDropboxMgrMove.query.count()
        self.assertEqual(after_db_count, before_db_count + 1)
        m = MavenDropboxMgrMove.query.first()
        tdiff = m.when_moved - utcnow
        self.assertTrue(tdiff.seconds < 10)
        self.assertEqual(m.src_filename, src_fn)
        self.assertEqual(m.dest_filename, dest_fn)
        self.assertEqual(m.md5, hashlib.md5(open(dest_fn).read().encode()).hexdigest())
        self.assertEqual(m.file_size, os.path.getsize(dest_fn))

    def test_move_file(self):
        utcnow = time_utilities.utc_now().replace(tzinfo=None)  # sqlite can't store timezone info

        formatted_time = utcnow.strftime('%Y%m%dT%H%M%S')
        year_str = utcnow.strftime('%Y')
        month_str = utcnow.strftime('%m')
        dest_dir = os.path.join(
            config.root_destination_directory, 'test', 'ql')

        os.makedirs(dest_dir)
        assert os.path.isdir(dest_dir)
        bn = 'mvn_test_ql_descriptor_' + formatted_time + '_v02_r01.png'
        src_fn = os.path.join(self.root_source_directory, bn)
        with open(src_fn, 'w') as f:
            f.write('just something to fill the file')
        before_db_count = MavenDropboxMgrMove.query.count()

        utilities.move_valid_dropbox_file(src_fn,
                                          self.invalid_dir,
                                          self.dupe_dir)
        # it disappeared from the source directory
        self.assertFalse(os.path.exists(src_fn))
        dest_fn = os.path.join(dest_dir, year_str, month_str, bn)
        # and appeared in the destination directory
        self.assertTrue(os.path.isfile(dest_fn))
        after_db_count = MavenDropboxMgrMove.query.count()
        self.assertEqual(after_db_count, before_db_count + 1)
        m = MavenDropboxMgrMove.query.first()
        tdiff = m.when_moved - utcnow
        self.assertTrue(tdiff.seconds < 10)
        self.assertEqual(m.src_filename, src_fn)
        self.assertEqual(m.dest_filename, dest_fn)
        self.assertEqual(m.md5, hashlib.md5(open(dest_fn).read().encode()).hexdigest())
        self.assertEqual(m.file_size, os.path.getsize(dest_fn))

    def test_move_invalid_base(self):
        original_root = config.root_destination_directory
        config.root_destination_directory = '/tmp/bogus'
        utcnow = time_utilities.utc_now().replace(tzinfo=None)  # sqlite can't store timezone info

        formatted_time = utcnow.strftime('%Y%m%dT%H%M%S')
        bn = 'mvn_test_ql_invalidbase_' + formatted_time + '_v02_r01.png'
        src_fn = os.path.join(self.root_source_directory, bn)
        with open(src_fn, 'w') as f:
            f.write('just something to fill the file')
        try:
            utilities.move_valid_dropbox_file(src_fn,
                                              self.invalid_dir,
                                              self.dupe_dir)
            assert False, 'An exception was expected.  The root directory %s should not exist' % config.root_destination_directory
        except Exception:
            pass
        finally:
            # clean up test file
            os.remove(src_fn)
            # restore root_destination_directory
            config.root_destination_directory = original_root

    def test_move_invalid_filenames(self):
        self.move_invalid_filename('some-bogus-name.png')
        self.move_invalid_filename('just-a-few_underscores.png')
        self.move_invalid_filename(
            'mvn_test_invalid-instrument_descriptor_2000909T010101_v02_r01.png')

    def fail_side_effect(self, err_msg):
        self.fail("Unexpected Invocation.  Message : " + err_msg)

    def move_invalid_filename(self, invalid_file_name):
        ''' Test the ability to detect invalid instrument and quicklook files
            if an invalid file is found, that invalid file should be moved outside
            of the dropbox and into an invalid directory '''
        src_dir = os.path.join(self.root_source_directory, 'test')
        dest_dir = os.path.join(
            config.root_destination_directory, 'test', 'test')
        invalid_dir = os.path.join(
            self.root_source_directory, config.invalid_dir_name)

        try:
            # disable old_enough checking
            config.age_limit = 0.0

            mail.send_critical_email = Mock(side_effect=self.fail_side_effect)
            os.makedirs(src_dir)
            os.makedirs(dest_dir)
            assert os.path.isdir(src_dir)
            assert os.path.isdir(dest_dir)

            src_fn = os.path.join(src_dir, invalid_file_name)
            with open(src_fn, 'w') as f:
                f.write('just something to fill the file')
            before_db_count = MavenDropboxMgrMove.query.count()

            utilities.move_files_in_directory_tree(src_dir)
            # it disappeared from the source directory
            self.assertFalse(os.path.exists(src_fn))
            dest_fn = os.path.join(invalid_dir, invalid_file_name)
            # and appeared in the destination directory
            self.assertTrue(os.path.isfile(dest_fn))
            after_db_count = MavenDropboxMgrMove.query.count()
            # db count should remain unchanged
            self.assertEqual(after_db_count, before_db_count)
        finally:
            if os.path.isdir(dest_dir):
                shutil.rmtree(dest_dir)
            if os.path.isdir(src_dir):
                shutil.rmtree(src_dir)

    def test_move_metadata_files(self):

        expected_dest_dir = os.path.join(
            config.root_destination_directory, 'ngi', config.metadata_dir_name)
        dest_dir = os.path.join(config.root_destination_directory, 'ngi')

        os.makedirs(dest_dir)
        assert os.path.isdir(dest_dir)
        bn = 'collection_ngims_context.csv'
        transformed_name = config.metadata_filename_transform(
            file_pattern.matches_on_group([maven_config.metadata_regex], bn))
        src_fn = os.path.join(self.root_source_directory, bn)
        with open(src_fn, 'w') as f:
            f.write('just something to fill the file')
        before_db_count = MavenDropboxMgrMove.query.count()

        utilities.move_valid_dropbox_file(src_fn,
                                          self.invalid_dir,
                                          self.dupe_dir)
        # it disappeared from the source directory
        self.assertFalse(os.path.exists(src_fn))
        dest_fn = os.path.join(expected_dest_dir, transformed_name)
        # and appeared in the destination directory
        self.assertTrue(os.path.isfile(dest_fn))
        after_db_count = MavenDropboxMgrMove.query.count()
        self.assertEqual(after_db_count, before_db_count + 1)
        m = MavenDropboxMgrMove.query.first()
        self.assertEqual(m.src_filename, src_fn)
        self.assertEqual(m.dest_filename, dest_fn)
        self.assertEqual(m.md5, hashlib.md5(open(dest_fn).read().encode()).hexdigest())
        self.assertEqual(m.file_size, os.path.getsize(dest_fn))

    def test_move_caveats_files(self):
        expected_dest_dir = os.path.join(
            config.root_destination_directory, 'ngi', config.metadata_dir_name)
        dest_dir = os.path.join(config.root_destination_directory, 'ngi')

        os.makedirs(dest_dir)
        assert os.path.isdir(dest_dir)
        bn = 'ngims_tid_caveats_v01_r02.pdf'
        transformed_name = config.caveats_filename_transform(
            file_pattern.matches_on_group([maven_config.metadata_caveats_regex], bn))
        src_fn = os.path.join(self.root_source_directory, bn)
        with open(src_fn, 'w') as f:
            f.write('just something to fill the file')
        before_db_count = MavenDropboxMgrMove.query.count()

        utilities.move_valid_dropbox_file(src_fn,
                                          self.invalid_dir,
                                          self.dupe_dir)
        # it disappeared from the source directory
        self.assertFalse(os.path.exists(src_fn))
        dest_fn = os.path.join(expected_dest_dir, transformed_name)
        # and appeared in the destination directory
        self.assertTrue(os.path.isfile(dest_fn))
        after_db_count = MavenDropboxMgrMove.query.count()
        self.assertEqual(after_db_count, before_db_count + 1)
        m = MavenDropboxMgrMove.query.first()
        self.assertEqual(m.src_filename, src_fn)
        self.assertEqual(m.dest_filename, dest_fn)
        self.assertEqual(m.md5, hashlib.md5(open(dest_fn).read().encode()).hexdigest())
        self.assertEqual(m.file_size, os.path.getsize(dest_fn))

    def test_move_sis_files(self):

        dest_dir = os.path.join(config.root_destination_directory, 'ngi')
        expected_dest_dir = os.path.join(dest_dir, config.metadata_dir_name)

        os.makedirs(dest_dir)
        assert os.path.isdir(dest_dir)
        bn = 'ngims_pds_sis.pdf'
        transformed_name = config.sis_filename_transform(
            file_pattern.matches_on_group([maven_config.sis_regex], bn))
        src_fn = os.path.join(self.root_source_directory, bn)
        with open(src_fn, 'w') as f:
            f.write('just something to fill the file')
        before_db_count = MavenDropboxMgrMove.query.count()

        utilities.move_valid_dropbox_file(src_fn,
                                          self.invalid_dir,
                                          self.dupe_dir)
        # it disappeared from the source directory
        self.assertFalse(os.path.exists(src_fn))
        dest_fn = os.path.join(expected_dest_dir, transformed_name)
        # and appeared in the destination directory
        self.assertTrue(os.path.isfile(dest_fn))
        after_db_count = MavenDropboxMgrMove.query.count()
        self.assertEqual(after_db_count, before_db_count + 1)
        m = MavenDropboxMgrMove.query.first()
        self.assertEqual(m.src_filename, src_fn)
        self.assertEqual(m.dest_filename, dest_fn)
        self.assertEqual(m.md5, hashlib.md5(open(dest_fn).read().encode()).hexdigest())
        self.assertEqual(m.file_size, os.path.getsize(dest_fn))

    def test_move_readme_files(self):

        dest_dir = os.path.join(config.root_destination_directory, 'ngi')
        expected_dest_dir = os.path.join(dest_dir, config.metadata_dir_name)

        os.makedirs(dest_dir)
        assert os.path.isdir(dest_dir)
        bn = 'ngims_readme.txt'
        transformed_name = config.readme_filename_transform(
            file_pattern.matches_on_group([maven_config.metadata_readme_regex], bn))
        self.assertTrue(maven_config.metadata_index_regex.match(transformed_name))
        src_fn = os.path.join(self.root_source_directory, bn)
        with open(src_fn, 'w') as f:
            f.write('just something to fill the file')
        before_db_count = MavenDropboxMgrMove.query.count()

        utilities.move_valid_dropbox_file(src_fn,
                                          self.invalid_dir,
                                          self.dupe_dir)
        # it disappeared from the source directory
        self.assertFalse(os.path.exists(src_fn))
        dest_fn = os.path.join(expected_dest_dir, transformed_name)
        # and appeared in the destination directory
        self.assertTrue(os.path.isfile(dest_fn))
        after_db_count = MavenDropboxMgrMove.query.count()
        self.assertEqual(after_db_count, before_db_count + 1)
        m = MavenDropboxMgrMove.query.first()
        self.assertEqual(m.src_filename, src_fn)
        self.assertEqual(m.dest_filename, dest_fn)
        self.assertEqual(m.md5, hashlib.md5(open(dest_fn).read().encode()).hexdigest())
        self.assertEqual(m.file_size, os.path.getsize(dest_fn))

    def test_move_version_changes_files(self):

        dest_dir = os.path.join(config.root_destination_directory, 'ngi')
        expected_dest_dir = os.path.join(dest_dir, config.metadata_dir_name)

        os.makedirs(dest_dir)
        assert os.path.isdir(dest_dir)
        bn = 'ngims_product_version_changes_v01_r02.pdf'
        transformed_name = config.version_changes_filename_transform(
            file_pattern.matches_on_group([maven_config.metadata_version_changes_regex], bn))
        self.assertTrue(maven_config.metadata_index_regex.match(transformed_name))
        src_fn = os.path.join(self.root_source_directory, bn)
        with open(src_fn, 'w') as f:
            f.write('just something to fill the file')
        before_db_count = MavenDropboxMgrMove.query.count()

        utilities.move_valid_dropbox_file(src_fn,
                                          self.invalid_dir,
                                          self.dupe_dir)
        # it disappeared from the source directory
        self.assertFalse(os.path.exists(src_fn))
        dest_fn = os.path.join(expected_dest_dir, transformed_name)
        # and appeared in the destination directory
        self.assertTrue(os.path.isfile(dest_fn))
        after_db_count = MavenDropboxMgrMove.query.count()
        self.assertEqual(after_db_count, before_db_count + 1)
        m = MavenDropboxMgrMove.query.first()
        self.assertEqual(m.src_filename, src_fn)
        self.assertEqual(m.dest_filename, dest_fn)
        self.assertEqual(m.md5, hashlib.md5(open(dest_fn).read().encode()).hexdigest())
        self.assertEqual(m.file_size, os.path.getsize(dest_fn))

    def test_move_metadata_description_files(self):

        dest_dir = os.path.join(config.root_destination_directory, 'ngi')
        expected_dest_dir = os.path.join(dest_dir, config.metadata_dir_name)

        os.makedirs(dest_dir)
        assert os.path.isdir(dest_dir)
        bn = 'collection_ngi_context_with_description.csv'
        transformed_name = config.metadata_filename_transform(
            file_pattern.matches_on_group([maven_config.metadata_regex], bn))
        src_fn = os.path.join(self.root_source_directory, bn)
        with open(src_fn, 'w') as f:
            f.write('just something to fill the file')
        before_db_count = MavenDropboxMgrMove.query.count()

        utilities.move_valid_dropbox_file(src_fn,
                                          self.invalid_dir,
                                          self.dupe_dir)
        # it disappeared from the source directory
        self.assertFalse(os.path.exists(src_fn))
        dest_fn = os.path.join(expected_dest_dir, transformed_name)
        # and appeared in the destination directory
        self.assertTrue(os.path.isfile(dest_fn))
        after_db_count = MavenDropboxMgrMove.query.count()
        self.assertEqual(after_db_count, before_db_count + 1)
        m = MavenDropboxMgrMove.query.first()
        self.assertEqual(m.src_filename, src_fn)
        self.assertEqual(m.dest_filename, dest_fn)
        self.assertEqual(m.md5, hashlib.md5(open(dest_fn).read().encode()).hexdigest())
        self.assertEqual(m.file_size, os.path.getsize(dest_fn))

    def test_transformed_name_is_metadata_indexed(self):
        '''Test to ensure the various transformed names meet the metadata_index_pattern regex'''

        # Collection/Bundle Metadata Files
        self.assertIsNone(file_pattern.matches_on_group(
            [maven_config.metadata_index_regex], 'bundle_maven_ngims.xml'))
        transformed_name = config.metadata_filename_transform(file_pattern.matches_on_group(
            [maven_config.metadata_regex], 'bundle_maven_ngims.xml'))  # no level given
        self.assertIn('nolevel', transformed_name)  # default level: mvn_ngi_nolevel_bundle_20171129T183400.xml vs. mvn_ngi__bundle_20171129T183549.xml
        self.assertIsNotNone(file_pattern.matches_on_group(
            [maven_config.metadata_index_regex], transformed_name), transformed_name)

        # level given below, does not default        
        transformed_name = config.metadata_filename_transform(file_pattern.matches_on_group(
            [maven_config.metadata_regex], 'collection_maven_iuv_processed_schema.xml'))
        self.assertNotIn('nolevel', transformed_name)
        self.assertIsNotNone(file_pattern.matches_on_group(
            [maven_config.metadata_index_regex], transformed_name))
        transformed_name = config.metadata_filename_transform(file_pattern.matches_on_group(
            [maven_config.metadata_regex], 'bundle_maven_iuv_processed.xml'))
        self.assertNotIn('nolevel', transformed_name)
        self.assertIsNotNone(file_pattern.matches_on_group(
            [maven_config.metadata_index_regex], transformed_name))
        transformed_name = config.metadata_filename_transform(file_pattern.matches_on_group(
            [maven_config.metadata_regex], 'collection_ngi_context.csv'))
        self.assertNotIn('nolevel', transformed_name)
        self.assertIsNotNone(file_pattern.matches_on_group(
            [maven_config.metadata_index_regex], transformed_name))
        transformed_name = config.metadata_filename_transform(file_pattern.matches_on_group(
            [maven_config.metadata_regex], 'collection_maven_iuvs_raw_calibration_inventory.tab'))
        self.assertNotIn('nolevel', transformed_name)
        self.assertIsNotNone(file_pattern.matches_on_group(
            [maven_config.metadata_index_regex], transformed_name))
        
        # SIS Metadata Files
        transformed_name = config.sis_filename_transform(
            file_pattern.matches_on_group([maven_config.sis_regex], 'ngims_pds_sis.pdf'))
        self.assertNotIn('nolevel', transformed_name)
        self.assertIsNotNone(file_pattern.matches_on_group(
            [maven_config.metadata_index_regex], transformed_name))

        # Caveat Metadata Files
        transformed_name = config.caveats_filename_transform(file_pattern.matches_on_group(
            [maven_config.metadata_caveats_regex], 'ngims_tid_caveats_v01_r02.pdf'))
        self.assertNotIn('nolevel', transformed_name)
        self.assertIsNotNone(file_pattern.matches_on_group(
            [maven_config.metadata_index_regex], transformed_name))
        transformed_name = config.caveats_filename_transform(file_pattern.matches_on_group(
            [maven_config.metadata_caveats_regex], 'ngims_tid_caveats_v01_r02.xml'))
        self.assertNotIn('nolevel', transformed_name)
        self.assertIsNotNone(file_pattern.matches_on_group(
            [maven_config.metadata_index_regex], transformed_name))
        transformed_name = config.caveats_filename_transform(file_pattern.matches_on_group(
            [maven_config.metadata_caveats_regex], 'ngims_tid_caveats.pdf'))
        self.assertNotIn('nolevel', transformed_name)
        self.assertIsNotNone(file_pattern.matches_on_group(
            [maven_config.metadata_index_regex], transformed_name))

        # Readme Metadata Files
        transformed_name = config.readme_filename_transform(file_pattern.matches_on_group(
            [maven_config.metadata_readme_regex], 'iuvs_data_readme.txt'))
        self.assertNotIn('nolevel', transformed_name)
        self.assertIsNotNone(file_pattern.matches_on_group(
            [maven_config.metadata_index_regex], transformed_name))
        transformed_name = config.readme_filename_transform(file_pattern.matches_on_group(
            [maven_config.metadata_readme_regex], 'ngims_readme.txt'))
        self.assertNotIn('nolevel', transformed_name)
        self.assertIsNotNone(file_pattern.matches_on_group(
            [maven_config.metadata_index_regex], transformed_name))

        # Product Version Metadata Files
        transformed_name = config.version_changes_filename_transform(file_pattern.matches_on_group(
            [maven_config.metadata_version_changes_regex], 'ngims_product_version_changes_v01_r02.pdf'))
        self.assertNotIn('nolevel', transformed_name)
        self.assertIsNotNone(file_pattern.matches_on_group(
            [maven_config.metadata_index_regex], transformed_name))

    def test_metadata_duplicates(self):
        expected_dest_dir = os.path.join(
            config.root_destination_directory, 'iuv', config.metadata_dir_name)
        dest_dir = os.path.join(config.root_destination_directory, 'iuv')
        os.makedirs(expected_dest_dir)
        assert os.path.isdir(dest_dir)
        
        # Create a file in the "dropbox"
        bn = 'collection_maven_iuvs_calibrated_limb_inventory.tab'
        transformed_name = config.metadata_filename_transform(
            file_pattern.matches_on_group([maven_config.metadata_regex], bn))
        src_fn = os.path.join(self.root_source_directory, bn)
        with open(src_fn, 'w') as f:
            f.write('just something to fill the file')
        
        # Create the file that is the same as bn
        dest_bn_1 = 'mvn_iuv_calibrated_collection_limb-inventory_20160404T000000.tab'
        dest_fn_1 = os.path.join(expected_dest_dir, dest_bn_1)
        with open(dest_fn_1, 'w') as f:
            f.write('just something to fill the file')
        
        # Create a file that is not the same as bn
        dest_bn_2 = 'mvn_iuv_calibrated_collection_limb-inventory_20150404T000000.tab'
        dest_fn_2 = os.path.join(expected_dest_dir, dest_bn_2)
        with open(dest_fn_2, 'w') as f:
            f.write('just something ELSE to fill the file')
            
        # Create an unrelated file to sit in the directory with a matching checksum
        dest_bn_3 = 'mvn_iuv_pds_sis_20151223T174401.pdf'
        dest_fn_3 = os.path.join(expected_dest_dir, dest_bn_3)
        with open(dest_fn_3, 'w') as f:
            f.write('just something to fill the file')
        
        # Create a non science file in the directory
        dest_bn_4 = 'maven-not-a-real-file.txt'
        dest_fn_4 = os.path.join(expected_dest_dir, dest_bn_4)
        with open(dest_fn_4, 'w') as f:
            f.write('just here to make sure nothing breaks')
        
        # Create another older file in the directory that is the same as bn
        dest_bn_5 = 'mvn_iuv_calibrated_collection_limb-inventory_20140404T000000.tab'
        dest_fn_5 = os.path.join(expected_dest_dir, dest_bn_5)
        with open(dest_fn_5, 'w') as f:
            f.write('just something to fill the file')
        
        # Attempt to move the file
        before_db_count = MavenDropboxMgrMove.query.count()
        utilities.move_valid_dropbox_file(src_fn,
                                          self.invalid_dir,
                                          self.dupe_dir)
        
        # Check that src_fn disappeared from the source directory
        self.assertFalse(os.path.exists(src_fn))
        
        # Check that the dest_bn files still exist
        self.assertTrue(os.path.exists((dest_fn_1)))
        self.assertTrue(os.path.exists((dest_fn_2)))
        self.assertTrue(os.path.exists((dest_fn_3)))
        self.assertTrue(os.path.exists((dest_fn_3)))
        self.assertTrue(os.path.exists((dest_fn_4)))
        self.assertTrue(os.path.exists((dest_fn_5)))
        
        # Check that src_fn was NOT moved to the destination directory
        dest_fn = os.path.join(expected_dest_dir, transformed_name)
        self.assertFalse(os.path.isfile(dest_fn))
        after_db_count = MavenDropboxMgrMove.query.count()
        self.assertEqual(after_db_count, before_db_count)

    def test_metadata_non_duplicates(self):
        expected_dest_dir = os.path.join(
            config.root_destination_directory, 'iuv', config.metadata_dir_name)
        dest_dir = os.path.join(config.root_destination_directory, 'iuv')
        os.makedirs(expected_dest_dir)
        assert os.path.isdir(dest_dir)
        
        bn = 'collection_maven_iuvs_calibrated_limb_inventory.tab'
        transformed_name = config.metadata_filename_transform(
            file_pattern.matches_on_group([maven_config.metadata_regex], bn))
        src_fn = os.path.join(self.root_source_directory, bn)
        with open(src_fn, 'w') as f:
            f.write('just something to fill the file')

        # Create a file that is different from bn
        # Should remain because basename differs
        dest_bn_1 = 'mvn_iuv_calibrated_collection_limb-inventory1_20160404T000000.tab'
        dest_fn_1 = os.path.join(expected_dest_dir, dest_bn_1)
        with open(dest_fn_1, 'w') as f:
            f.write('just something different to fill the file')
        
        # Create another file that is different from bn
        # Should be updated (effectively removed, because timetag is updated to now)
        dest_bn_2 = 'mvn_iuv_calibrated_collection_limb-inventory_20150404T000000.tab'
        dest_fn_2 = os.path.join(expected_dest_dir, dest_bn_2)
        with open(dest_fn_2, 'w') as f:
            f.write('just something else to fill the file')

        # Create an unrelated file to sit in the directory with a matching checksum
        dest_bn_3 = 'mvn_iuv_pds_sis_20151223T174401.pdf'
        dest_fn_3 = os.path.join(expected_dest_dir, dest_bn_3)
        with open(dest_fn_3, 'w') as f:
            f.write('just something to fill the file')
            
        # Create a non science file in the directory
        dest_bn_4 = 'maven-not-a-real-file.txt'
        dest_fn_4 = os.path.join(expected_dest_dir, dest_bn_4)
        with open(dest_fn_4, 'w') as f:
            f.write('just here to make sure nothing breaks')
        
        # Create another older file in the directory that is the same as bn
        # Should remain.  Both bn and content match
        dest_bn_5 = 'mvn_iuv_calibrated_collection_limb-inventory_20140404T000000.tab'
        dest_fn_5 = os.path.join(expected_dest_dir, dest_bn_5)
        with open(dest_fn_5, 'w') as f:
            f.write('just something to fill the file')
                     
        before_db_count = MavenDropboxMgrMove.query.count()

        utilities.move_valid_dropbox_file(src_fn,
                                          self.invalid_dir,
                                          self.dupe_dir)
        
        # Check that src_fn disappeared from the source directory
        self.assertFalse(os.path.exists(src_fn))
        
        # Check that the dest_bn files still exist
        self.assertTrue(os.path.exists((dest_fn_1)))
        self.assertFalse(os.path.exists((dest_fn_2)))
        self.assertTrue(os.path.exists((dest_fn_3)))
        self.assertTrue(os.path.exists((dest_fn_3)))
        self.assertTrue(os.path.exists((dest_fn_4)))
        self.assertTrue(os.path.exists((dest_fn_5)))
        
        # Check that src_fn was moved to the destination directory                 
        dest_fn = os.path.join(expected_dest_dir, transformed_name)
        self.assertTrue(os.path.isfile(dest_fn))
        after_db_count = MavenDropboxMgrMove.query.count()
        self.assertEqual(after_db_count, before_db_count + 1)
        m = MavenDropboxMgrMove.query.first()
        self.assertEqual(m.src_filename, src_fn)
        self.assertEqual(m.dest_filename, dest_fn)
        self.assertEqual(m.md5, hashlib.md5(open(dest_fn).read().encode()).hexdigest())
        self.assertEqual(m.file_size, os.path.getsize(dest_fn))

    def test_metadata_same_size_duplicates(self):
        expected_dest_dir = os.path.join(
            config.root_destination_directory, 'iuv', config.metadata_dir_name)
        dest_dir = os.path.join(config.root_destination_directory, 'iuv')
        os.makedirs(expected_dest_dir)
        assert os.path.isdir(dest_dir)
        
        bn = 'collection_maven_iuvs_calibrated_limb_inventory.tab'
        transformed_name = config.metadata_filename_transform(
            file_pattern.matches_on_group([maven_config.metadata_regex], bn))
        src_fn = os.path.join(self.root_source_directory, bn)
        with open(src_fn, 'w') as f:
            f.write('just something to fill the file')

        # Create a file that is different from bn
        # BUT it has the same file size
        dest_bn_1 = 'mvn_iuv_calibrated_collection_limb-inventory1_20160404T000000.tab'
        dest_fn_1 = os.path.join(expected_dest_dir, dest_bn_1)
        with open(dest_fn_1, 'w') as f:
            f.write('this will be the same file size')
        
        # Make sure they're the same size
        self.assertTrue(os.stat(src_fn).st_size == os.stat(dest_fn_1).st_size)
        
        # Attempt to move the file
        before_db_count = MavenDropboxMgrMove.query.count()
        utilities.move_valid_dropbox_file(src_fn,
                                          self.invalid_dir,
                                          self.dupe_dir)
        
        # Check that src_fn disappeared from the source directory
        self.assertFalse(os.path.exists(src_fn))
        
        # Check that dest_fn_1 file still exists
        self.assertTrue(os.path.exists((dest_fn_1)))

        # Check that src_fn was moved to the destination directory                 
        dest_fn = os.path.join(expected_dest_dir, transformed_name)
        self.assertTrue(os.path.isfile(dest_fn))
        after_db_count = MavenDropboxMgrMove.query.count()
        self.assertEqual(after_db_count, before_db_count + 1)
        m = MavenDropboxMgrMove.query.first()
        self.assertEqual(m.src_filename, src_fn)
        self.assertEqual(m.dest_filename, dest_fn)
        self.assertEqual(m.md5, hashlib.md5(open(dest_fn).read().encode()).hexdigest())
        self.assertEqual(m.file_size, os.path.getsize(dest_fn))

    def test_misnamed_check(self):
        '''Run tests to ensure misnamed files don't end up under the source directory'''

        misnamed_file = 'this_is_misnamed.for.sure'
        self.run_misnamed_file_check(misnamed_file)

        misnamed_file = 'add/some/dir/structure/this_is_misnamed.for.sure'
        self.run_misnamed_file_check(misnamed_file)

    def test_check_filename_transform_logger(self):

        dest_dir = os.path.join(config.root_destination_directory, 'ngi')

        os.makedirs(dest_dir)
        assert os.path.isdir(dest_dir)
        bn = 'collection_ngims_context.csv'
        transformed_name = config.metadata_filename_transform(
            file_pattern.matches_on_group([maven_config.metadata_regex], bn))
        src_fn = os.path.join(self.root_source_directory, bn)
        with open(src_fn, 'w') as f:
            f.write('just something to fill the file')

        utilities.move_valid_dropbox_file(src_fn,
                                          self.invalid_dir,
                                          self.dupe_dir)

        ifile = open(constants.filename_transforms_location, 'r')
        reader = csv.reader(ifile)
        transform_dict = {}
        for row in reader:
            transform_dict[row[0]] = row[1]

        self.assertTrue(transformed_name in transform_dict)
        self.assertEqual(transform_dict[transformed_name], bn)

    def run_misnamed_file_check(self, source_file):
        '''Method used to run a misnamed file check on the provided file.
        This method will ensure the misnamed file is moved to a location outside of
        the source directory'''

        # disable old_enough checking
        config.age_limit = 0.0

        # Create source file
        src_file = os.path.join(self.root_source_directory, source_file)
        parent, bn = os.path.split(src_file)
        # does the src_file parent dir exist?  If not, create
        if not os.path.isdir(parent):
            os.makedirs(parent)

        with open(src_file, 'w') as f:
            f.write('just something to fill the file')
        # run dropbox
        utilities.move_files_in_directory_tree(self.root_source_directory)
        # it disappeared from the source directory
        self.assertFalse(os.path.exists(src_file))
        dst_file = self.find_file(bn, self.test_root)
        self.assertIsNotNone(dst_file)
        # ensure it didn't end up under the destination directory
        self.assertIsNone(self.find_file(bn, config.root_destination_directory))

    def find_file(self, base_file_name, base_path):
        '''Helper method used to find a file from a given base path'''
        for root, _, files in os.walk(base_path):
            if base_file_name in files:
                return os.path.join(root, base_file_name)
        return None
