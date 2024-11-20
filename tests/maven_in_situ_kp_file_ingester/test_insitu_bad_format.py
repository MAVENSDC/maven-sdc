'''
Created on Jul 30, 2015

@author: cosc3564
'''
import unittest
import os
from multiprocessing import Lock
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
from maven_in_situ_kp_file_ingester.in_situ_kp_file_processor import insitu_file_processor
from tests.maven_test_utilities import generate_format
from tests.maven_test_utilities.db_utils import delete_data
from tests.maven_test_utilities.file_system import get_temp_root_dir
from maven_database.models import KpFilesMetadata
from maven_database.database import db_session, engine
from shutil import rmtree


class TestInSituBadFormat(unittest.TestCase):
    '''Testing for exceptions raised by mismatch formats and data lines'''

    def setUp(self):
        self.parent_formats = 0
        self.test_dir = get_temp_root_dir()
        self.file_name = self.test_dir + '/mvn_kp_insitu_20141011_v01_r01.tab'

        with open(self.file_name, 'w') as f:
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'PARAMETER', 'INSTRUMENT', 'UNITS', 'COLUMN #', 'FORMAT', 'NOTES'))
            f.write('# Comment\n')
            # Add the time format
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Time (UTC/SCET)', 'instrument', 'unit', 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Spacecraft GEO X', '      ', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Spacecraft GEO Y', '      ', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Spacecraft GEO Z', '      ', 'km', self.parent_formats + 1, 'format', 'notes'))
            # write a comment line
            f.write('# \n')

    def tearDown(self):
        delete_data()
        rmtree(self.test_dir)
        self.assertFalse(os.path.isdir(self.test_dir))

    def testExceptionDataTypes(self):
        data_1 = [0]
        data_2 = [0, 1]
        data_3 = [0, 1, 2]
        data_4 = [0, 1, 2, 3]

        self.runFormatException(data_1)
        self.runFormatException(data_2)
        self.runFormatException(data_3)
        self.runFormatException(data_4)

    def runFormatException(self, data_format):
        with open(self.file_name, 'a') as f:
            f.write(generate_format.inSituGenerateDataLine(
                time_utilities.utc_now(), data_format))

        with self.assertRaises(Exception) as context:
            with insitu_file_processor(self.file_name, db_session, engine, Lock()) as processor:
                processor.ingest_data()
        # exception: '# formats processed [len(format_data)] != # data values
        # [len(datum[1]) on line number: 6'
        self.assertIn('8', str(context.exception))
        self.assertEqual(processor.kp_files_metadata.ingest_status, 'FAILED')
        current_kp_status = KpFilesMetadata.query.filter(
            (KpFilesMetadata.ingest_status == 'FAILED')).all()
        self.assertEqual('FAILED', current_kp_status[0].ingest_status)
