'''
Created on Sep 5, 2014

@author: Bryan Staley
@author Kristi Entzel
'''
import unittest
import os
from datetime import timedelta
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
from monitoring import new_files
from tests.maven_test_utilities import file_system, db_utils


class TestNewFiles(unittest.TestCase):
    '''
     Unit tests for monitoring.new_files
    '''

    def setUp(self):
        '''
          Setup test data with a directory hierarchy 
        '''
        self.base_test_path = file_system.get_temp_root_dir()
        self.file_root = os.path.join(self.base_test_path, 'foo_root.txt')
        # this creates the file and closes the file handle
        with open(self.file_root, 'a+'):
            pass

        self.test_time = time_utilities.utc_now()
        self.files = [(os.path.join(self.base_test_path, 'root_file.txt'), self.test_time + timedelta(hours=1)),
                      (os.path.join(self.base_test_path, 'dir1', 'dir1_file.txt'), self.test_time + timedelta(hours=2)),
                      (os.path.join(self.base_test_path, 'dir2', 'dir2_file.txt'), self.test_time + timedelta(hours=3))]

        for _next in self.files:
            db_utils.insert_science_files_metadata(file_name=os.path.basename(_next[0]),
                                                   dir_path=os.path.dirname(_next[0]),
                                                   mod_date=_next[1])

    def tearDown(self):
        '''
        Clean up of test data
        '''
        db_utils.delete_data()

    def test_new_files_without_subdirs(self):
        '''
        Find files that are only visible from root
        '''
        # Without subdirectories set to true, list_files will only find the files in the directory given.
        results = new_files.list_files(self.base_test_path, False, 10)
        self.assertEqual(1, len(results),
                         "list_files didn't return the correct number of files!")
        self.assertEqual(2, len(results[0]))

    def test_new_files(self):
        '''
        Find files at all levels
        '''
        results = new_files.list_files(self.base_test_path, True, 10)

        self.assertEqual(len(self.files), len(results),
                         "list_files didn't return the correct number of files!")
        self.assertTrue(all(len(pair) == 2 for pair in results))

        shortened_results = new_files.list_files(self.base_test_path, True, 2)

        self.assertEqual(2, len(shortened_results),
                         "list_files didn't return the correct number of files!")
        self.assertTrue(all(len(pair) == 2 for pair in shortened_results))

        for name, _ in results:
            if name not in [f for f, _ in self.files]:
                self.fail('The file %s was not in return values' % name)

    def test_main(self):
        '''
        Test the main function
        '''
        # List of paths that don't include the a/2 directory.
        path_list = ','.join([self.base_test_path,
                              os.path.join(self.base_test_path, 'dir1')])

        new_files.main(path_list, True, self.file_root, 30)
        with open(self.file_root, 'r') as testFileSubdirs:
            test_str = '<a href="%s/dir2/dir2_file.txt">/' % (self.base_test_path)
            self.assertIn(test_str, testFileSubdirs.read())

        new_files.main(path_list, False, self.file_root, 30)
        with open(self.file_root, 'r') as testFile:
            test_str = '<a href="%s/dir2/dir2_file.txt">/' % (self.base_test_path)
            self.assertNotIn(test_str, testFile.read())

    def test_write_html(self):
        '''
        Test the Write function
        '''
        from io import StringIO

        output = StringIO()
        test_str = 'p;%s<br />\n<a href="%s/dir2/dir2_file.txt">/' % (self.files[1][1], self.base_test_path)
        file_array = [(fn, str(time)) for fn, time in self.files]
        new_files.write_html(file_array, output)
        self.assertNotEqual(0, os.path.getsize(self.base_test_path))
        self.assertIn(test_str, output.getvalue())
