'''
Created on Mar 5, 2015

@author: bstaley
'''
import os
from time import time
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'

unit_test_base_dir = '/tmp/maventest'
default_maven_test_user = 'maventest'


def get_temp_root_dir(user_env='MAVEN_TEST_USER', use_time_stamp=True, create=True):
    '''Utility method used to create a tmp directory from environment variables with optional timestamp'''
    user = os.environ.get(user_env, default_maven_test_user)
    test_dir_root = os.path.join(unit_test_base_dir, user)
    if use_time_stamp:
        utc_stamp_millisec = str(int(round(time() * 100000)))
        test_dir_root = os.path.join(test_dir_root, utc_stamp_millisec)
    if not os.path.isdir(test_dir_root) and create:
        os.makedirs(test_dir_root)
    return test_dir_root


def build_test_files_and_structure(default_file_contents, files_base_dir, files_list):
    ''' Utility method used to build the directory structure and contents for
    the provided instrument files '''
    test_files = []
    for next_file in files_list:
        fully_qualified_file_name = os.path.join(files_base_dir, next_file)
        if not os.path.exists(os.path.dirname(fully_qualified_file_name)):
            os.makedirs(os.path.dirname(fully_qualified_file_name))
        with open(fully_qualified_file_name, 'w') as f:
            f.write(default_file_contents)
        test_files.append(fully_qualified_file_name)
    return test_files


def remove_all_files_in_directory(directory_to_clear):
    for f in [f for f in os.listdir(directory_to_clear) if os.path.isfile(os.path.join(directory_to_clear, f))]:
        os.remove(os.path.join(directory_to_clear, f))
