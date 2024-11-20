'''
Created on Jan 25, 2016

@author: bstaley
'''
import unittest
import os
from datetime import datetime
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from tests.maven_test_utilities import db_utils
from maven_database.database import engine, db_session
from maven_database.models import ScienceFilesMetadata


class ScienceFilesMetadataLatestPerformanceTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        db_utils.delete_data(ScienceFilesMetadata)

    @unittest.skip('skipping')
    def testManyShallowFamilies(self):
        file_roots = ['mvn_test_many-shallow-families-{0}'.format(i) for i in range(10000)]
        self.populate_science_files(file_roots, range(2), range(2))

        query = '''SELECT file_root 
                FROM science_files_metadata 
                WHERE file_root = ?
                GROUP BY file_root 
                ORDER BY MAX(absolute_version) DESC
                '''

        start = datetime.now()
        print(self.getLatestFromORM(file_roots[0] + '.txt'))
        print('ORM took', datetime.now() - start)

        start = datetime.now()
        print(self.getLatestFromCursor(query, (format(file_roots[0] + '.txt'))))
        print('cursor took', datetime.now() - start)

        start = datetime.now()
        print(self.getLatestFromORM(file_roots[0] + '.txt'))
        print('ORM took', datetime.now() - start)

        # ensure nobody benefits from server side caching
        start = datetime.now()
        print(self.getLatestFromCursor(query, (format(file_roots[0] + '.txt'))))
        print('cursor took', datetime.now() - start)

    # @unittest.skip('skipping')
    def testManyShallowFamiliesManyResults(self):
        file_roots = ['mvn_test_many-shallow-families-{0}'.format(i) for i in range(100)]
        self.populate_science_files(file_roots, range(2), range(2))

        query = '''
                SELECT file_root 
                FROM science_files_metadata 
                WHERE file_root = ?
                GROUP BY file_root 
                ORDER BY MAX(absolute_version) DESC
                '''.format(file_roots[0] + '.txt')

        start = datetime.now()
        print(self.getAllLatestFromORM(file_roots[0] + '.txt'))
        print('ORM took', datetime.now() - start)

        start = datetime.now()
        print(self.getAllLatestFromCursor(query, (file_roots[0] + '.txt')))
        print('cursor took', datetime.now() - start)

        start = datetime.now()
        print(self.getAllLatestFromORM(file_roots[0] + '.txt'))
        print('ORM took', datetime.now() - start)

        # ensure nobody benefits from server side caching
        start = datetime.now()
        print(self.getAllLatestFromCursor(query, (file_roots[0] + '.txt')))
        print('cursor took', datetime.now() - start)

    # @unittest.skip('skipping')
    def testFewDeepFamilies(self):
        file_roots = ['mvn_test_many-shallow-families-{0}'.format(i) for i in range(100)]
        self.populate_science_files(file_roots, range(20), range(20))

        query = '''
                SELECT file_root 
                FROM science_files_metadata 
                WHERE file_root = ?
                GROUP BY file_root 
                ORDER BY MAX(absolute_version) DESC
                '''

        start = datetime.now()
        print(self.getLatestFromCursor(query, (format(file_roots[0] + '.txt'))))
        print('cursor took', datetime.now() - start)

        start = datetime.now()
        print(self.getLatestFromORM(file_roots[0] + '.txt'))
        print('ORM took', datetime.now() - start)

        # ensure nobody benefits from server side caching
        start = datetime.now()
        print(self.getLatestFromCursor(query, (format(file_roots[0] + '.txt'))))
        print('cursor took', datetime.now() - start)

        start = datetime.now()
        print(self.getLatestFromORM(file_roots[0] + '.txt'))
        print('ORM took', datetime.now() - start)

    def getLatestFromCursor(self, query, parameters):
        cursor = engine.execution_options().execute(query, parameters)
        return cursor.first()

    def getLatestFromORM(self, file_root):

        query = db_session.query(ScienceFilesMetadata.file_root).filter(ScienceFilesMetadata.file_root == file_root).order_by(ScienceFilesMetadata.absolute_version.desc()).distinct()
        return query.first()

    def getAllLatestFromCursor(self, query, parameters):
        cursor = engine.execution_options().execute(query, parameters)
        return [x for x in cursor]

    def getAllLatestFromORM(self, file_root):

        query = (db_session.query(ScienceFilesMetadata.file_root).
                 order_by(ScienceFilesMetadata.absolute_version.desc()).distinct())
        return query.all()

    def populate_science_files(self, file_roots, versions, revisions):
        for next_root in file_roots:
            for next_version in versions:
                for next_revision in revisions:
                    db_utils.insert_science_files_metadata(file_name='{0}_v{1}_r{2}.txt'.
                                                           format(next_root, next_version, next_revision))
