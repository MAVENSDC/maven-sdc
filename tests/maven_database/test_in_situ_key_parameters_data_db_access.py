import os
import unittest
from datetime import datetime
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from maven_database import db_session
from maven_database.models import KpFilesMetadata, InSituKpQueryParameter, InSituKeyParametersData
from tests.maven_test_utilities import db_utils


class InSituKeyParametersDataDbAccessTestCase(unittest.TestCase):

    def setUp(self):
        self.test_kp_files_metadata = KpFilesMetadata('test_file',
                                                      '/path/to/nowhere',
                                                      0,
                                                      'test_instr',
                                                      datetime.now(),
                                                      2,
                                                      1)
        db_session.add(self.test_kp_files_metadata)
        db_session.commit()

        self.test_in_situ_kp_query_parameter = InSituKpQueryParameter('test_query_param', 'test_instrument', 'test_column_name', 'test_format',
                                                                       'test_units', 'test_notes')
        db_session.add(self.test_in_situ_kp_query_parameter)
        db_session.commit()

    def tearDown(self):
        db_utils.delete_data(KpFilesMetadata, InSituKpQueryParameter, InSituKeyParametersData)

    def smoke_test(self):
        self.assertTrue(True)

    def test_insert(self):
        '''Test that string formats are handled'''
        test_time = datetime.now()
        before_count = InSituKeyParametersData.query.count()

        self.assertEqual(0, before_count)

        test_kp = InSituKeyParametersData(kp_files_metadata_id=self.test_kp_files_metadata.id,
                                          timetag=test_time,
                                          file_line_number=42,
                                          in_situ_kp_query_parameters_id=self.test_in_situ_kp_query_parameter.id,
                                          data_value=52.0)
        db_session.add(test_kp)
        db_session.commit()

        after_count = InSituKeyParametersData.query.count()
        self.assertEqual(after_count, before_count + 1)
        q = InSituKeyParametersData.query.first()
        self.assertEqual(test_time, q.timetag)
        self.assertEqual(42.0, q.file_line_number)
        self.assertEqual(52.0, q.data_value)
        self.assertEqual(str(q), '%s %s' % (q.id, q.data_value))