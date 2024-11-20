import os
import unittest
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
import maven_database
from maven_database.models import KpFilesMetadata
import random


class KpFilesMetadataDbAccessTestCase(unittest.TestCase):
    '''Tests access of the database table that holds the key
    parameter files metadata.
    '''

    def setUp(self):
        self.file_name = 'test_file'
        self.directory_path = '/path/to/nowhere'
        self.file_size = random.randint(1, 1234234)
        self.file_type = 'test_file'
        self.timetag = time_utilities.utc_now().replace(tzinfo=None)
        self.version = 2
        self.revision = 1

    def tearDown(self):
        rows = KpFilesMetadata.query.all()
        for r in rows:
            maven_database.db_session.delete(r)
        maven_database.db_session.commit()
        maven_database.db_session.close()

    def test_for_smoke(self):
        self.assertTrue(True)

    def test_insert(self):
        '''Test that string formats are handled'''
        before_count = KpFilesMetadata.query.count()
        m = KpFilesMetadata(self.file_name,
                            self.directory_path,
                            self.file_size,
                            self.file_type,
                            self.timetag,
                            self.version,
                            self.revision)
        maven_database.db_session.add(m)
        maven_database.db_session.commit()
        after_count = KpFilesMetadata.query.count()
        self.assertEqual(after_count - 1, before_count)
        m = KpFilesMetadata.query.first()
        self.assertEqual(self.file_name, m.file_name)
        self.assertEqual(self.directory_path, m.directory_path)
        self.assertEqual(self.file_size, m.file_size)
        self.assertEqual(self.file_type, m.file_type)
        self.assertEqual(self.timetag, m.timetag)
        self.assertEqual(self.version, m.version)
        self.assertEqual(self.revision, m.revision)
        string_rep = KpFilesMetadata(m.file_name,
                                     m.directory_path,
                                     m.file_size,
                                     m.file_type,
                                     m.timetag,
                                     m.version,
                                     m.revision,
                                     m.ingest_status)
        self.assertEqual(str(string_rep), "%s %s %d" % (m.directory_path, m.file_name, m.file_size))