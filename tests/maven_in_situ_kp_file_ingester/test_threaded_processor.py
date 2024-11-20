'''
Created on Jul 3, 2015

@author: bussell
'''
import os
import unittest
from shutil import rmtree
from maven_database.models import InSituKeyParametersData, InSituKpQueryParameter, KpFilesMetadata, MavenStatus
from maven_in_situ_kp_file_ingester.config import pool_size
from maven_in_situ_kp_file_ingester.utilities import ingest_in_situ_kp_data
from maven_status import MAVEN_SDC_COMPONENT, MAVEN_SDC_EVENTS
from tests.maven_test_utilities.db_utils import delete_data
from tests.maven_test_utilities.file_system import get_temp_root_dir
from tests.maven_test_utilities import generate_format
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'


class ThreadedProcessorTestCase(unittest.TestCase):

    def setUp(self):
        self.test_root_dir = get_temp_root_dir()
        self.test_fp_list = [
            os.path.join(
                self.test_root_dir, 'mvn_kp_insitu_2015050{0}_v02_r03.tab'.format(n))
            for n in range(1, pool_size + 1)
        ]
        self.num_formats = 26
        self.num_samples = 400
        for fp in self.test_fp_list:
            with open(fp, 'w') as f:
                f.write(generate_format.inSituGenerateFormatLine(
                    None, 'PARAMETER', 'INSTRUMENT', 'UNITS', 'COLUMN #', 'FORMAT', 'NOTES'))
                f.write('# Comment\n')
                f.write(generate_format.inSituGenerateFormatLine(None, 'Time (UTC/SCET)', 'instrument',
                                                                 'unit', 1, 'format', 'notes'))
                for i in range(1, self.num_formats + 1):
                    f.write(generate_format.inSituGenerateFormatLine(None, 'Magnetic Field MSO X' + str(i),
                                                                     'MAG', 'nT', i + 1, 'format', 'notes'))

                data = [i for i in range(self.num_formats)]
                f.write('# \n')
                for i in range(0, self.num_samples):
                    f.write(generate_format.inSituGenerateDataLine(
                        time_utilities.utc_now(), data))

    def tearDown(self):
        delete_data()
        rmtree(self.test_root_dir)
        self.assertFalse(os.path.isdir(self.test_root_dir))

    def test_threaded_in_situ_processor(self):
        expected_data_count = self.num_formats * self.num_samples * pool_size
        ingest_in_situ_kp_data(self.test_root_dir)

        kp_metadata_count = KpFilesMetadata.query.count()
        kp_data_count = InSituKeyParametersData.query.count()
        kp_query_count = InSituKpQueryParameter.query.filter(
            InSituKpQueryParameter.instrument_name != 'Derived').count()
        
        self.assertEqual(pool_size, kp_metadata_count)
        self.assertEqual(expected_data_count, kp_data_count)
        self.assertEqual(self.num_formats, kp_query_count)
