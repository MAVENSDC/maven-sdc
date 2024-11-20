import os
import unittest
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from maven_database import db_session
from maven_database.models import InSituKpQueryParameter


class InSituKpQueryParameterDbAccessTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        for row in InSituKpQueryParameter.query.all():
            db_session.delete(row)
        db_session.commit()

    def smoke_test(self):
        self.assertTrue(True)

    def test_insert(self):
        '''Test that string formats are handled'''
        before_count = InSituKpQueryParameter.query.count()
        qp = InSituKpQueryParameter('test_query_param', 'test_instrument', 'test_column_name', 'test_format',
                                    'test_units', 'test_notes')
        db_session.add(qp)
        db_session.commit()
        after_count = InSituKpQueryParameter.query.count()
        self.assertEqual(after_count, before_count + 1)
        q = InSituKpQueryParameter.query.first()
        self.assertEqual(q.query_parameter, 'test_query_param')
        self.assertEqual(q.instrument_name, 'test_instrument')
        self.assertEqual(q.kp_column_name, 'test_column_name')
        self.assertEqual(q.data_format, 'test_format')
        self.assertEqual(q.units, 'test_units')
        self.assertEqual(q.notes, 'test_notes')
        string_rep = InSituKpQueryParameter(q.query_parameter,
                                            q.instrument_name,
                                            q.kp_column_name,
                                            q.data_format,
                                            q.units,
                                            q.notes)
        self.assertEqual(str(string_rep), '%s => (%s, %s)' % (q.query_parameter, q.instrument_name, q.kp_column_name))