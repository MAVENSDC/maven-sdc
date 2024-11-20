
import os
import unittest
import datetime
import maven_database
from shutil import rmtree
from maven_database.models import KpFilesMetadata, MavenLog
from maven_in_situ_kp_file_ingester.utilities import ingest_entry_point
from maven_in_situ_kp_file_ingester import utilities
from tests.maven_test_utilities import file_system
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'


class UtilityFunctionsTestCase(unittest.TestCase):
    '''Tests utility functions.'''

    def setUp(self):
        self.test_root = file_system.get_temp_root_dir()

    def clearTables(self, *tables):
        for table in tables:
            rows = table.query.all()
            for r in rows:
                maven_database.db_session.delete(r)

        maven_database.db_session.commit()

    def tearDown(self):
        ''' NOTE - Many of the below KeyParameter types have a foreign key dependency on KpFilesMetadata
            ensure that KpFilesMetadata is removed last '''
        self.clearTables(KpFilesMetadata, MavenLog)
        rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))

    def test_is_in_situ_kp_file(self):
        fn = 'mvn_kp_insitu_20150108_v001_r04.tab'
        self.assertTrue(utilities.is_in_situ_kp_file(fn))

    def test_has_been_ingested(self):
        directory_path = '/path/to/nowhere'
        fn = 'mvn_kp_insitu_20150108_v001_r04.tab'
        file_path = os.path.join(directory_path, fn)
        self.assertFalse(utilities.has_been_ingested(file_path))
        m = KpFilesMetadata(fn,
                            directory_path,
                            0,
                            'test file',
                            datetime.datetime.now(),
                            2,
                            1)
        maven_database.db_session.add(m)
        maven_database.db_session.commit()
        self.assertTrue(not utilities.has_been_ingested(file_path))

        m.ingest_status = "COMPLETE"
        maven_database.db_session.flush()

        self.assertTrue(utilities.has_been_ingested(file_path))
        
    def test_ingest_entry_point(self):
        directory_path = '/path/to/nowhere'
        fn = 'mvn_kp_insitu_20150108_v001_r04.tab'
        file_path = os.path.join(directory_path, fn)
        self.assertFalse(utilities.has_been_ingested(file_path))
        _, value = ingest_entry_point(fn)
        
        # given file is not a file
        self.assertEqual(value, -1) # update status to ERROR
        