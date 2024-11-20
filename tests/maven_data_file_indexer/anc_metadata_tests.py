'''Tests of maven_data_file_indexer with ancillary files.

Created on Mar 10, 2015

@author: Kim Kokkonen
'''
import sys
sys.path.append('../../maven_data_file_indexer')
import os
import unittest
from datetime import datetime
from shutil import rmtree
import pytz
from maven_utilities import constants, anc_config, file_pattern
os.environ[constants.python_env] = 'testing'
from maven_data_file_indexer import maven_file_indexer, utilities as indexer_utilities, audit_utilities
from tests.maven_test_utilities import file_system, db_utils
from tests.maven_test_utilities.db_utils import delete_data
from maven_database.models import AncillaryFilesMetadata


class AncillaryMetadataTestCase(unittest.TestCase):
    '''Tests computation of metadata for ancillary files.'''

    def setUp(self):
        self.root_directory = file_system.get_temp_root_dir(create=False)
        self.directory_path = os.path.join(self.root_directory, 'path/to/nowhere')
        if not os.path.isdir(self.directory_path):
            os.makedirs(self.directory_path)
        assert os.path.isdir(self.directory_path)

        anc_filename = 'sci_anc_eps14_365_001.drf'
        self.filename = os.path.join(self.directory_path, anc_filename)
        self.file_size = 1234
        with open(self.filename, 'w') as f:
            f.write('a' * self.file_size)

        imu_filename = 'mvn_imu14_322_329.txt'
        self.imu_filename = os.path.join(self.directory_path, imu_filename)
        self.imu_file_size = 567
        with open(self.imu_filename, 'w') as f:
            f.write('a' * self.imu_file_size)

        alt_anc_filename = 'sci_anc_ngms_2013_325_326.drf'
        self.alt_anc_filename = os.path.join(self.directory_path, alt_anc_filename)
        self.alt_anc_size = 567
        with open(self.alt_anc_filename, 'w') as f:
            f.write('a' * self.alt_anc_size)

    def tearDown(self):
        rmtree(self.root_directory)
        self.assertFalse(os.path.isdir(self.root_directory))
        delete_data(AncillaryFilesMetadata)

    def test_anc_filename_regex(self):
        filenames = ['sci_anc_eps14_364_365.drf', 'sci_anc_eps15_002_003.drf'
                                                  'sci_anc_gnc13_337_338.drf', 'sci_anc_ngms15_063_066.drf',
                     'sci_anc_pf14_006_007.drf', 'sci_anc_pte14_322_329.drf',
                     'sci_anc_rs13_337_338.drf', 'sci_anc_sasm114_193_195.drf',
                     'sci_anc_sasm115_043_044.drf', 'sci_anc_sasm214_281_282.drf',
                     'sci_anc_sasm315_014_017.drf', 'sci_anc_usm113_365_001.drf',
                     'sci_anc_usm115_063_066.drf', 'sci_anc_usm515_002_003.drf',
                     'sci_anc_eps_2013_324_325.drf', 'sci_anc_gnc_2013_324_325.drf',
                     'sci_anc_pte21_019_025.drf'
                     ]
        for fn in filenames:
            m = file_pattern.matches([anc_config.ancillary_regex], fn)
            self.assertTrue(m,f'{fn} does not meet regex')

        bad_filenames = ['sci_anc_eps14_286_287_manual.drf',
                         'eps_mag1_140706_140707_v03.drf',
                         'mvn_imu14_322_329.txt',
                         'sci_anc_ngms14_286_287_manual.drf',
                         'sci_anc_pf14_006_007_v2.drf',
                         'sci_anc_sasm1_140709_140711_v03.drf',
                         'mvn_rec_141006_141006_v03.sff',
                         'mvn_rec_150304_150307_updated_v03.sff',
                         'mvn_tst_141229_141230_deepdipsvt_v02.sff',
                         'sci_nom_svt_anc_usm5.drf']
        for fn in bad_filenames:
            m = file_pattern.matches([anc_config.ancillary_regex], fn)
            self.assertFalse(m)

    def test_imu_filename_regex(self):
        filenames = ['mvn_imu14_322_329.txt', 'mvn_imu15_063_066.txt', 'mvn_pte14_338_339.drf']
        for fn in filenames:
            m = file_pattern.matches([anc_config.ancillary_eng_regex], fn)
            self.assertTrue(m is not None)

        bad_filenames = ['sci_anc_eps14_364_365.drf',
                         'mvn_imu.dat.txt']
        for fn in bad_filenames:
            m = file_pattern.matches([anc_config.ancillary_eng_regex], fn)
            self.assertFalse(m)

    def test_anc_metadata_type(self):
        metadata = indexer_utilities.get_metadata_for_ancillary_file(self.filename)
        self.assertIn("AncFileMetadata", str(type(metadata)))
        self.assertTrue(isinstance(metadata, indexer_utilities.AncFileMetadata))
        indexer_utilities.upsert_ancillary_file_metadata([metadata])

        anc_metadata = AncillaryFilesMetadata.query.all()
        self.assertEqual(1, len(anc_metadata))
        db_utils.create_ancillary_metadata(product_list=['anctest1', 'anctest2'], root_dir=self.directory_path)
        anc_metadata = AncillaryFilesMetadata.query.all()
        self.assertEqual(3, len(anc_metadata))

    def test_anc_metadata_for_good_filename(self):
        metadata = indexer_utilities.get_metadata_for_ancillary_file(self.filename)
        self.assertTrue(metadata)
        self.assertEqual(metadata.directory_path, self.directory_path)
        self.assertEqual(metadata.file_name, os.path.basename(self.filename))
        self.assertEqual(metadata.file_size, os.path.getsize(self.filename))
        self.assertEqual(metadata.product, 'eps')
        start_date = pytz.utc.localize(datetime(2014, 12, 31, 0, 0, 0))
        end_date = pytz.utc.localize(datetime(2015, 1, 1, 0, 0, 0))
        self.assertEqual(metadata.start_date, start_date)
        self.assertEqual(metadata.end_date, end_date)
        self.assertEqual(metadata.file_extension, 'drf')

    def test_imu_metadata_for_good_filename(self):
        metadata = indexer_utilities.get_metadata_for_ancillary_file(self.imu_filename)
        self.assertTrue(metadata)
        self.assertEqual(metadata.directory_path, self.directory_path)
        self.assertEqual(metadata.file_name, os.path.basename(self.imu_filename))
        self.assertEqual(metadata.file_size, os.path.getsize(self.imu_filename))
        self.assertEqual(metadata.product, 'imu')
        start_date = pytz.utc.localize(datetime(2014, 11, 18, 0, 0, 0))
        end_date = pytz.utc.localize(datetime(2014, 11, 25, 0, 0, 0))
        self.assertEqual(metadata.start_date, start_date)
        self.assertEqual(metadata.end_date, end_date)
        self.assertEqual(metadata.file_extension, 'txt')

    def test_alt_anc_metadata_for_good_filename(self):
        metadata = indexer_utilities.get_metadata_for_ancillary_file(self.alt_anc_filename)
        self.assertTrue(metadata)
        self.assertEqual(metadata.directory_path, self.directory_path)
        self.assertEqual(metadata.file_name, os.path.basename(self.alt_anc_filename))
        self.assertEqual(metadata.file_size, os.path.getsize(self.alt_anc_filename))
        self.assertEqual(metadata.product, 'ngms')
        start_date = pytz.utc.localize(datetime(2013, 11, 21, 0, 0, 0))
        end_date = pytz.utc.localize(datetime(2013, 11, 22, 0, 0, 0))
        self.assertEqual(metadata.start_date, start_date)
        self.assertEqual(metadata.end_date, end_date)
        self.assertEqual(metadata.file_extension, 'drf')

    @unittest.skip('skipping')
    def test_l2_metadata_for_bad_filenames(self):
        # cannot contain a valid imu filename
        bad_filenames = [  # 'sci_anc_eps_2013_324_325.drf',
            'sci_anc_eps14_286_287_manual.drf',
            'eps_mag1_140706_140707_v03.drf',
            'sci_anc_gnc_2013_324_325.drf',
            'sci_anc_ngms14_286_287_manual.drf',
            'sci_anc_pf14_006_007_v2.drf',
            'mvn_pte14_338_339.drf',
            'sci_anc_sasm1_140709_140711_v03.drf',
            'mvn_rec_141006_141006_v03.sff',
            'mvn_rec_150304_150307_updated_v03.sff',
            'mvn_tst_141229_141230_deepdipsvt_v02.sff',
            'sci_nom_svt_anc_usm5.drf']
        for fn in bad_filenames:
            metadata = indexer_utilities.get_metadata_for_ancillary_file(fn)
            self.assertFalse(metadata)

    def test_metadata_insert(self):
        metadata = indexer_utilities.get_metadata_for_ancillary_file(self.filename)
        indexer_utilities.insert_ancillary_file_metadatum(metadata)
        self.assertEqual(AncillaryFilesMetadata.query.count(), 1)
        db_metadata = AncillaryFilesMetadata.query.first()

        self.assertEqual(db_metadata.directory_path, metadata.directory_path)
        self.assertEqual(db_metadata.file_name, metadata.file_name)
        self.assertEqual(db_metadata.file_size, metadata.file_size)

        imu_metadata = indexer_utilities.get_metadata_for_ancillary_file(self.imu_filename)
        indexer_utilities.insert_ancillary_file_metadatum(imu_metadata)
        self.assertEqual(AncillaryFilesMetadata.query.count(), 2)
        imu_db_metadata = AncillaryFilesMetadata.query.filter(AncillaryFilesMetadata.product == 'imu').first()

        self.assertEqual(imu_db_metadata.directory_path, imu_metadata.directory_path)
        self.assertEqual(imu_db_metadata.file_name, imu_metadata.file_name)
        self.assertEqual(imu_db_metadata.file_size, imu_metadata.file_size)

    def test_get_ancillary_files_metadata(self):
        metadata = audit_utilities.get_metadata_from_disk(self.root_directory)
        self.assertIsNotNone(metadata)
        # none of the files are in the database
        self.assertEqual(len(metadata), 3)
        m_filenames = [os.path.basename(m.path_name) for m in metadata]
        self.assertTrue(os.path.basename(self.filename) in m_filenames)
        self.assertTrue(os.path.basename(self.imu_filename) in m_filenames)

    def test_update_ancillary_files(self):
        maven_file_indexer.run_full_index([self.root_directory])
        self.assertEqual(AncillaryFilesMetadata.query.count(), 3)
        db_metadata = AncillaryFilesMetadata.query.all()
        db_filenames = [m.file_name for m in db_metadata]
        self.assertTrue(os.path.basename(self.filename) in db_filenames)
        self.assertTrue(os.path.basename(self.imu_filename) in db_filenames)

        # now we shouldn't try to insert again
        disk_meta = audit_utilities.get_metadata_from_disk(self.root_directory)
        db_meta = audit_utilities.get_metadata_from_db(self.root_directory)
        add, _, _ = audit_utilities.get_metadata_diffs(db_meta, disk_meta)
        self.assertIsNotNone(add)
        self.assertEqual(len(add), 0)

    def test_get_dict_of_ancillary_file_names_on_disk(self):
        result = [x.path_name for x in audit_utilities.get_metadata_from_disk(self.root_directory)]
        self.assertIn(self.filename, result)
        self.assertIn(self.imu_filename, result)
