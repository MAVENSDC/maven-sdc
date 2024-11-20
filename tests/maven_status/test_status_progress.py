'''
Created on Sep 22, 2016

@author: bstaley
'''
import unittest
from maven_database.models import MavenStatus
from tests.maven_test_utilities import db_utils
from maven_status import MAVEN_SDC_COMPONENT, progress


class TestStatusProgress(unittest.TestCase):

    def setUp(self):
        self.component_id = MAVEN_SDC_COMPONENT.DROPBOX

    def tearDown(self):
        db_utils.delete_data()

    def testProgressHandler(self):
        test_handler = progress.StatusProgressHandler(self.component_id, product='test product')

        for i in range(10):
            test_handler.handle(i if i % 2 else 0, i if not i % 2 else 0, 10)
        
        results = MavenStatus.query.order_by(MavenStatus.id).all()
        self.assertIn("Total Files: 10", str(results))

        self.assertEqual(10, len(results))
        
        for i in range(10):
            self.assertIn(str(float(i) / float(10) * 100.0), results[i].summary)

        for i in range(10):
            test_handler.handle(i if i % 2 else 0, i if not i % 2 else 0, 0)
        no_files = MavenStatus.query.order_by(MavenStatus.id).all()
        self.assertIn("Total Files: 0", str(no_files))
