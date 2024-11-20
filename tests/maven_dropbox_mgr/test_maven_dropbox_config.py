'''
Created on Oct 17, 2019

@author: bstaley
'''
import unittest
import os
from shutil import rmtree
from tests.maven_test_utilities import file_system
from maven_dropbox_mgr import config


class TestMavenDropboxConfig(unittest.TestCase):

    def setUp(self):
        self.root_source_directory = file_system.get_temp_root_dir()

    def tearDown(self):
        rmtree(self.root_source_directory)
        self.assertFalse(os.path.isdir(self.root_source_directory))

    def testDuplicateCheck(self):
        
        truth_dir = os.path.join(self.root_source_directory, 'truth')
        test_dir = os.path.join(self.root_source_directory, 'test')
        # Add a couple test files to the directory system
        test_files = ['test1.txt', 'test2.txt']
        test_file_contents = 'a' * 1024
        test_file_full_names = file_system.build_test_files_and_structure(default_file_contents=test_file_contents,
                                                                          files_base_dir=truth_dir,
                                                                          files_list=test_files)
        
        # Name and Contents match
        check_f = test_files[0]
        check_file_full_names = file_system.build_test_files_and_structure(default_file_contents=test_file_contents,
                                                                           files_base_dir=test_dir,
                                                                           files_list=[check_f])
        
        result = config.file_duplicate_check(src_fn=test_file_full_names[0],
                                             dst_fn=check_file_full_names[0])
        
        self.assertEqual((config.DUPLICATE_ACTION.REMOVE, None), result)
        
        # Name match
        check_f = test_files[1]
        check_file_full_names = file_system.build_test_files_and_structure(default_file_contents=test_file_contents + 'b',
                                                                           files_base_dir=test_dir,
                                                                           files_list=[check_f])
        
        result = config.file_duplicate_check(src_fn=test_file_full_names[1],
                                             dst_fn=check_file_full_names[0])
        
        self.assertEqual((config.DUPLICATE_ACTION.OVERWRITE_ARCHIVE, check_file_full_names[0]), result)
        
        # No match
        check_f = 'test3.txt'
        check_file_full_names = file_system.build_test_files_and_structure(default_file_contents=test_file_contents + 'b',
                                                                           files_base_dir=test_dir,
                                                                           files_list=[check_f])
        
        result = config.file_duplicate_check(src_fn=test_file_full_names[1],
                                             dst_fn=check_file_full_names[0])
        
        self.assertEqual((config.DUPLICATE_ACTION.IGNORE, None), result)
        
