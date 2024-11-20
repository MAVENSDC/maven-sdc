'''
Created on May 18, 2015

@author: tbussell
'''
import sys
sys.path.append('../../maven_utilities')
import unittest
import os
import time
import tarfile
from shutil import rmtree
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from maven_utilities.utilities import listdir_files, file_is_old_enough, get_file_root_plus_extension, is_compressed_format, get_absolute_version, files_are_same
from tests.maven_test_utilities.file_system import get_temp_root_dir


class MavenUtilitiesUnitTests(unittest.TestCase):

    def setUp(self):
        self.test_root = get_temp_root_dir()
        self.src_directory = os.path.join(self.test_root, 'src')
        self.dst_directory = os.path.join(self.test_root, 'dst')
        os.mkdir(self.src_directory)
        os.mkdir(self.dst_directory)

    def tearDown(self):
        rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))
    
    def testCompressedFormat(self):
        '''utilities.is_compressed_format should determine if file is application/x-gzip type'''
        filename = os.path.join(self.src_directory,'foo.txt')
        with open(filename,'w') as f:f.write('foo')
        format_type_compressed = is_compressed_format(filename)
        self.assertFalse(format_type_compressed)
        
        filename = 'boo.tar'
        tmp_dir = os.path.join(self.test_root, 'tar')
        os.mkdir(tmp_dir)
        self.assertTrue(os.path.isdir(tmp_dir))
        path = os.path.join(tmp_dir, filename)
        with tarfile.open(path, 'w:gz') as tar:
            tar.add(tmp_dir, arcname=os.path.basename(path))
        self.assertTrue(os.path.isfile(path))
        
        format_type_compressed = is_compressed_format(path)
        self.assertTrue(format_type_compressed) 

    def testFileIsOldEnough(self):
        '''utilities.file_is_old_enough should behave correctly'''
        filename = 'mvn_foo_grp_l0_20121109.dat'
        path = os.path.join(self.src_directory, filename)
        f = open(path, 'w')
        f.close()
        self.assertFalse(file_is_old_enough(path, 300))
        os.utime(path, (time.time(), time.time() - 300 - 0.1))  # make the file old enough
        self.assertTrue(file_is_old_enough(path, 300))

    def testListdirFiles(self):
        '''utilities.listdir_files should only return the files that are in a directory'''
        fn = os.path.join(self.src_directory, 'test_file')
        with (open(fn, 'w')) as f:
            f.write('test content')
        dn = os.path.join(self.src_directory, 'test_dir')
        os.mkdir(dn)
        filenames = listdir_files(self.src_directory)
        self.assertTrue('test_file' in filenames)
        self.assertFalse('test_dir' in filenames)

        recursive_filenames = listdir_files(self.src_directory, recursive=True)
        self.assertNotEqual(recursive_filenames, [])
        self.assertIn('test_file', recursive_filenames)

    def test_get_file_root_plus_extension(self):
        fn = 'mvn_iuv_l1a_fuv-ISON2-corner-cycle06_20131215T030323_v00_r00.fits.gz'
        fn2 = 'mvn_ngi_l2_ion-abund-14630_20150113t215140_v02_r02.xml'
        fn3 = 'some_stuff_blah1234_v01.extension'
        fn4 = 'this_doesnt_matchnope'
        self.assertEqual(get_file_root_plus_extension(fn), 'mvn_iuv_l1a_fuv-ISON2-corner-cycle06_20131215T030323.fits.gz')
        self.assertEqual(get_file_root_plus_extension(fn2), 'mvn_ngi_l2_ion-abund-14630_20150113t215140.xml')
        self.assertEqual(get_file_root_plus_extension(fn3), 'some_stuff_blah1234.extension')
        self.assertEqual(get_file_root_plus_extension(fn4), None)

    def testGetAbsoluteVersion(self):
        '''utilities.get_absolute_version should return correct absolute version
        for different version and revision values
        '''
        version = None
        revision = None
        ab_version = get_absolute_version(version, revision)
        self.assertEqual(ab_version, 0)
        
        version = 1
        revision = None
        ab_version = get_absolute_version(version, revision)
        self.assertEqual(ab_version, 1000)
        
        version = None
        revision = 1
        ab_version = get_absolute_version(version, revision)
        self.assertEqual(ab_version, 1)
    
    def testFilesAreSame(self):
        '''utilities.files_are_different should be True if they are different and False if not.'''
        import string
        import random
        choices = string.ascii_letters + string.digits + string.punctuation
        contents = ''.join([random.choice(choices) for i in range(10000)])
        src_filename = 'mvn_foo_grp_l0_20121109.dat'
        src_path = os.path.join(self.src_directory, src_filename)
        with open(src_path, 'w') as f:
            f.write(contents)
        dest_filename = 'mvn_foo_grp_l0_20121109_v001.dat'
        dest_path = os.path.join(self.dst_directory, dest_filename)
        with open(dest_path, 'w') as f:
            f.write(contents)
        self.assertTrue(files_are_same(src_path, dest_path))
        with open(src_path, 'a') as f:
            f.write('a little bit more')
        self.assertFalse(files_are_same(src_path, dest_path))
