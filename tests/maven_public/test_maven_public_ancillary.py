'''
Created on Dec 16, 2015

@author: bstaley
'''
import unittest
import os
from shutil import rmtree
import pytz
from datetime import timedelta, datetime
from maven_utilities import constants
from sqlalchemy import and_
os.environ[constants.python_env] = 'testing'

from maven_database.models import AncillaryFilesMetadata, MavenOrbit
from maven_public import utilities as maven_public_utils
from maven_data_file_indexer import utilities as maven_file_indexer_utils
from tests.maven_test_utilities import file_system, db_utils


class TestPublicAncillary(unittest.TestCase):

    def setUp(self):
        # copy list
        self.source_generators = maven_public_utils.default_source_generators[:]

        self.test_root = file_system.get_temp_root_dir()
        self.site_root = os.path.join(self.test_root, 'site')
        os.mkdir(self.site_root)
        self.data_root = os.path.join(self.test_root, 'data')
        self.anc_data_root = os.path.join(self.data_root, 'anc')

        self.latest_anc_files = ['142660200SC202DSS34_noHdr.234',
                                 'anc_sci_svt_2015_215.drf',
                                 'mvn_rec_150118_150118_v02.sff',
                                 'mvn_tst_150118_150118_launchsvt_v01.sff',
                                 'mvn_tst_150226_150226_v10.sff',
                                 'sci_anc_eps14_286_287_manual.drf',
                                 'mvn_anc_trk_15166_15180.tgz',
                                 ]
        
        
        self.spice_data_root = os.path.join(self.data_root, 'anc/spice')
        self.all_spice_files = ['mvn_app_pred_141210_141210_ISON_v01.bc',
                                 'mvn_app_tst_141229_141229_svt13128_v01.bc',
                                 'mvn_iuv_all_l0_20131204_v042.bc',
                                 'MVN_SCLKSCET.00000.tsc',
                                 'mvn_sc_pred_150203_150203_tcm1_v01.bc',
                                 'mvn_sc_rec_150118_150118_v02.bc',
                                 'mvn_sc_rel_150118_150124_v01.bc',
                                 'mvn_sc_tst_150129_150129_svt13128_v01.bc',
                                 'spk_orb_141028-160101_130808_mvn.bsp',
                                 'spk_orb_141028-160101_130808_mvn.bsp.lbl',
                                 'trj_orb_00001-00001_00004_v9.bsp',
                                 'trj_orb_00001-00001_rec_v1.bsp',
                                 'mvn_iuv_all_l0_20131204_v001.bc',
                                 'trj_orb_00001-00001_00004_v8.bsp',
                                 ]
        
        self.optg_data_root = os.path.join(self.data_root, 'anc/optg')
        self.all_optg_files = ['optg_orb_00001-00001_00004_v1.txt',
                                 'optg_orb_00001-00001_rec_v1.txt',]
        
        self.old_anc_files = ['mvn_tst_150226_150226_v00.sff', ]

        self.all_anc_files = self.latest_anc_files + self.old_anc_files
        # Build ancillary files
        file_system.build_test_files_and_structure(default_file_contents='something to fill the file',
                                                   files_base_dir=self.anc_data_root,
                                                   files_list=self.all_anc_files)
        # Build spice files
        file_system.build_test_files_and_structure(default_file_contents='something to fill the file',
                                                   files_base_dir=self.spice_data_root,
                                                   files_list=self.all_spice_files)
        # Build optg files
        file_system.build_test_files_and_structure(default_file_contents='something to fill the file',
                                                   files_base_dir=self.optg_data_root,
                                                   files_list=self.all_optg_files)

        # Add orbit data
        db_utils.insert_orbit(number=1,
                              periapse=maven_public_utils.public_release_window_date_end - 
                              timedelta(days=1),
                              apoapse=maven_public_utils.public_release_window_date_end - timedelta(days=1))
        # Add ancillary files to metadata
        for anc_metadata in maven_file_indexer_utils.generate_metadata_for_ancillary_file([os.path.join(self.anc_data_root, f) for f in self.all_anc_files]):
            db_utils.insert_ancillary_file_metadata(file_name=anc_metadata.file_name,
                                                    base_name=anc_metadata.base_name,
                                                    directory_path=anc_metadata.directory_path,
                                                    file_size=anc_metadata.file_size,
                                                    product=anc_metadata.product,
                                                    file_extension=anc_metadata.file_extension,
                                                    mod_date=anc_metadata.mod_date,
                                                    start_date=anc_metadata.start_date,
                                                    end_date=anc_metadata.end_date,
                                                    version=anc_metadata.version)
            
        # Add spice files to metadata
        for spice_metadata in maven_file_indexer_utils.generate_metadata_for_ancillary_file([os.path.join(self.spice_data_root, f) for f in self.all_spice_files]):
            db_utils.insert_ancillary_file_metadata(file_name=spice_metadata.file_name,
                                                    base_name=spice_metadata.base_name,
                                                    directory_path=spice_metadata.directory_path,
                                                    file_size=spice_metadata.file_size,
                                                    product=spice_metadata.product,
                                                    file_extension=spice_metadata.file_extension,
                                                    mod_date=spice_metadata.mod_date,
                                                    start_date=spice_metadata.start_date,
                                                    end_date=spice_metadata.end_date,
                                                    version=spice_metadata.version)
            
        # Add optg files to metadata
        for optg_metadata in maven_file_indexer_utils.generate_metadata_for_ancillary_file([os.path.join(self.optg_data_root, f) for f in self.all_optg_files]):
            db_utils.insert_ancillary_file_metadata(file_name=optg_metadata.file_name,
                                                    base_name=optg_metadata.base_name,
                                                    directory_path=optg_metadata.directory_path,
                                                    file_size=optg_metadata.file_size,
                                                    product=optg_metadata.product,
                                                    file_extension=optg_metadata.file_extension,
                                                    mod_date=optg_metadata.mod_date,
                                                    start_date=optg_metadata.start_date,
                                                    end_date=optg_metadata.end_date,
                                                    version=optg_metadata.version)

        self.start_time = datetime(2014, 2, 23)
        self.end_time = datetime(2014, 8, 21)

    def tearDown(self):
        rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))
        db_utils.delete_data(MavenOrbit, AncillaryFilesMetadata)
        maven_public_utils.default_source_generators = self.source_generators

    def testPopulateAncillaryAll(self):
        for next_gen in maven_public_utils.default_source_generators:
            if isinstance(next_gen, maven_public_utils.AncQueryMetadata) or isinstance(next_gen, maven_public_utils.SciQueryMetadata):
                next_gen.latest = False
        for next_gen in maven_public_utils.default_source_generators:
            if isinstance(next_gen, maven_public_utils.SystemFileQueryMetadata):
                next_gen.root_dir = self.anc_data_root

        maven_public_utils.build_site(self.site_root, self.data_root, True)

        # assert all links exist.
        self.assertTrue(os.path.exists(
            os.readlink(os.path.join(self.site_root, maven_public_utils.ANC_DIR))))
        self.assertTrue(os.path.exists(
            os.readlink(os.path.join(self.site_root, maven_public_utils.SPICE_DIR))))
        self.assertTrue(os.path.exists(
            os.readlink(os.path.join(self.site_root, maven_public_utils.OPTG_DIR))))

        for next_public_file in self.all_anc_files:
            self.assertTrue(os.path.exists(os.readlink(
                os.path.join(self.site_root, maven_public_utils.ANC_DIR, next_public_file))))
            
        for next_public_file in self.all_spice_files:
            self.assertTrue(os.path.exists(os.path.join(self.site_root, maven_public_utils.SPICE_DIR, next_public_file)))
            
        for next_public_file in self.all_optg_files:
            self.assertTrue(os.path.exists(os.path.join(self.site_root, maven_public_utils.OPTG_DIR, next_public_file)))

    def testPopulateAncillaryLatest(self):
        for next_gen in maven_public_utils.default_source_generators:
            if isinstance(next_gen, maven_public_utils.AncQueryMetadata) or isinstance(next_gen, maven_public_utils.SciQueryMetadata):
                next_gen.latest = True
        for next_gen in maven_public_utils.default_source_generators:
            if isinstance(next_gen, maven_public_utils.SystemFileQueryMetadata):
                next_gen.root_dir = self.anc_data_root

        maven_public_utils.build_site(self.site_root, self.data_root, True)

        # assert all links exist.
        self.assertTrue(os.path.exists(
            os.readlink(os.path.join(self.site_root, maven_public_utils.ANC_DIR))))
        self.assertTrue(os.path.exists(
            os.readlink(os.path.join(self.site_root, maven_public_utils.SPICE_DIR))))
        self.assertTrue(os.path.exists(
            os.readlink(os.path.join(self.site_root, maven_public_utils.OPTG_DIR))))
        
        for next_public_file in self.latest_anc_files:
            self.assertTrue(os.path.exists(os.readlink(
                os.path.join(self.site_root, maven_public_utils.ANC_DIR, next_public_file))))

    def testAncQueryMetadataDates(self):
        '''Tests AncQueryMetadata class for file_extension, start_date, and version that is not None'''
        file_extension_types = ['234', 'drf', 'bc', 'sff', 'tsc', 'txt', 'bsp']
        anc_test = maven_public_utils.AncQueryMetadata(base_name=None,
                                                       product=None,
                                                       file_extension_list=file_extension_types,
                                                       start_date=self.start_time,
                                                       end_date=None,
                                                       version=1,
                                                       latest=False)

        timed_truth = AncillaryFilesMetadata.query.filter(and_(AncillaryFilesMetadata.version == 1,
                                                               AncillaryFilesMetadata.file_extension.in_(list(file_extension_types)),
                                                               AncillaryFilesMetadata.start_date >= self.start_time)).all()

        timed_test = anc_test.get_query()

        self.assertNotEqual(timed_truth, [], 'ancillary_query_metadata timed_truth is empty')
        for _next in timed_test.all():
            anfd = AncillaryFilesMetadata.query.filter(AncillaryFilesMetadata.file_name == _next[1]).first()
            print (anfd.file_name)
            self.assertGreaterEqual(anfd.start_date, self.start_time)
            self.assertIn(anfd.file_extension, file_extension_types)
            self.assertEqual(anfd.version, 1)
