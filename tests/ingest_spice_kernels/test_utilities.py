import os
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'

import unittest
from shutil import rmtree
import random
import time
import stat
import string
from maven_utilities.utilities import files_are_same
from ingest_spice_kernels import utilities
from ingest_spice_kernels import config
from tests.maven_test_utilities import file_system

import traceback
from mock import patch, Mock


class UtilitiesTestCase(unittest.TestCase):
    save_invalid_dir = config.invalid_dir
    misnamed_test_file = 'thisFileClearlyDoesntFitTheBill.junk'

    def setUp(self):
        self.test_root_directory = file_system.get_temp_root_dir()
        self.dest_directory = '%s/dest' % self.test_root_directory
        if not os.path.isdir(self.dest_directory):
            os.makedirs(self.dest_directory)
        config.invalid_dir = os.path.join(self.test_root_directory, 'misnamed_files')
        config.poc_root_source_dir = self.test_root_directory
        config.iuv_root_source_dir = os.path.join(self.test_root_directory, 'iuv')
        config.poc_root_dest_dir = os.path.join(self.test_root_directory, 'dst')
        if not os.path.isdir(config.invalid_dir):
            os.makedirs(config.invalid_dir)
        if not os.path.isdir(config.poc_root_source_dir):
            os.makedirs(config.poc_root_source_dir)
        if not os.path.isdir(config.poc_root_dest_dir):
            os.makedirs(config.poc_root_dest_dir)
        if not os.path.isdir(config.iuv_root_source_dir):
            os.makedirs(config.iuv_root_source_dir)

    def tearDown(self):
        rmtree(self.test_root_directory)
        self.assertFalse(os.path.isdir(self.test_root_directory))
        config.invalid_dir = self.save_invalid_dir

    def smoke_test(self):
        self.assertTrue(True)

    def test_increment_version(self):
        filename = 'mvn_test_spice_kernel_v001.bc'
        incremented_filename = config.increment_version(filename)
        self.assertEqual('mvn_test_spice_kernel_v002.bc', incremented_filename)

    def test_filename_exists_in_destination_directory(self):
        dest_filename = 'mvn_test_spice_kernel_v001.bc'
        self.assertFalse(config.filename_exists_in_destination_directory(self.dest_directory, dest_filename))
        dest_file_path = os.path.join(self.dest_directory, dest_filename)
        with open(dest_file_path, 'w') as f:
            pass  # create an empty file
        filename = 'mvn_test_spice_kernel.bc'  # try one without a version
        self.assertTrue(config.filename_exists_in_destination_directory(self.dest_directory, filename))
        filename = 'mvn_test_spice_kernel_v002.bc'  # try one with a version
        self.assertTrue(config.filename_exists_in_destination_directory(self.dest_directory, filename))

    def test_get_matching_dest_filename(self):
        dest_filename = 'mvn_test_spice_kernel_v001.bc'
        dest_file_path = os.path.join(self.dest_directory, dest_filename)
        with open(dest_file_path, 'w') as f:
            pass  # create an empty file
        filename = 'mvn_test_spice_kernel.bc'

        result = config.get_matching_dest_filename(self.dest_directory, filename)
        self.assertEqual(result, dest_filename)

        # create a file with a higher version and make sure that is the one that is returned
        dest_filename = 'mvn_test_spice_kernel_v002.bc'
        dest_file_path = os.path.join(self.dest_directory, dest_filename)
        with open(dest_file_path, 'w') as f:
            pass  # create an empty file
        result = config.get_matching_dest_filename(self.dest_directory, filename)
        self.assertEqual(result, dest_filename)

    def test_get_versioned_bc_destination_path(self):
        header_characters = [random.choice(string.ascii_letters + string.digits) for _ in range(9000)]
        body_characters = [random.choice(string.ascii_letters + string.digits) for _ in range(20000)]

        # Create a file in the destination directory.
        dest_filename = 'mvn_test_spice_kernel_v001.bc'
        dest_file_path = os.path.join(self.dest_directory, dest_filename)
        with open(dest_file_path, 'w') as f:
            f.write(''.join(header_characters))
            f.write(''.join(body_characters))

        # Create a file in the source directory that has a name that is the same as the root of the destination filename.
        filename = 'mvn_test_spice_kernel.bc'
        src_path = os.path.join(self.test_root_directory, filename)

        with open(src_path, 'w') as f:
            f.write(''.join(header_characters))
            f.write(''.join(body_characters))

        # body of both files are equal, so version in the destination filename is not changed
        paths_equal = config.get_versioned_bc_filename(self.test_root_directory, self.dest_directory, filename)
        self.assertEqual(paths_equal, 'mvn_test_spice_kernel_v001.bc')

        # Make the body in the source file different so that version in the destination filename is bumped.
        body_characters[1000] = chr(ord(body_characters[1000]) + 1)
        with open(src_path, 'w') as f:
            f.write(''.join(header_characters))
            f.write(''.join(body_characters))

        # Run the function under test.
        versioned_bc_dest_path = config.get_versioned_bc_filename(self.test_root_directory, self.dest_directory, filename)
        self.assertEqual(versioned_bc_dest_path, 'mvn_test_spice_kernel_v002.bc')

        # Test the case when the source file already has version.
        filename = 'mvn_test_spice_kernel_v01.bc'
        src_path = os.path.join(self.test_root_directory, filename)
        with open(src_path, 'w') as f:
            f.write(''.join(header_characters))
            f.write(''.join(body_characters))
        versioned_bc_dest_path = config.get_versioned_bc_filename(self.test_root_directory, self.dest_directory, filename)
        self.assertEqual(versioned_bc_dest_path, filename)  # the name was not changed, just the directory

    def test_file_contents_are_different(self):
        header_characters = [random.choice(string.ascii_letters + string.digits) for _ in range(9000)]
        body_characters = [random.choice(string.ascii_letters + string.digits) for _ in range(20000)]
        header1 = ''.join(header_characters)
        body1 = ''.join(body_characters)
        filename_1 = os.path.join(self.test_root_directory, 'filename_1')
        with open(filename_1, 'w') as f:
            f.write(header1)
            f.write(body1)

        # Change the header a little bit
        for i in range(8000, 8020):
            header_characters[i] = random.choice(string.ascii_letters + string.digits)
        header2 = ''.join(header_characters)

        # But keep the same body.
        body2 = ''.join(body_characters)
        filename_2 = os.path.join(self.test_root_directory, 'filename_2')
        with open(filename_2, 'w') as f:
            f.write(header2)
            f.write(body2)
        # this is also considered a different file
        self.assertFalse(files_are_same(filename_1, filename_2))

        # Change the body now. A difference in a single character should be discovered.
        body_characters[800] = chr(ord(body2[800]) + 1)
        body2 = ''.join(body_characters)
        with open(filename_2, 'w') as f:
            # use the original unchanged header
            f.write(header1)
            f.write(body2)
        self.assertFalse(files_are_same(filename_1, filename_2))

    def sendMailCheckForFailedFileInBodySideEffect(self, subject, message):
        self.assertTrue(self.misnamed_test_file in message)

    def testMovedInvalidFile(self):
        # TODO check if needed.
        with patch('maven_utilities.mail.send_email', new=Mock(side_effect=self.sendMailCheckForFailedFileInBodySideEffect)):

            src_path = os.path.join(self.test_root_directory, self.misnamed_test_file)
            with open(src_path, 'w') as f:
                f.write('testing')
            os.utime(src_path, (time.time(), time.time() - config.age_limit - 0.1))  # make the file old enough
            utilities.ingest_spice_files()  # move the file
            dest_path = os.path.join(config.invalid_dir, self.misnamed_test_file)
            self.assertFalse(os.path.isfile(src_path))  # the file was moved out of the source directory
            self.assertTrue(os.path.isfile(dest_path))  # the file was moved into the destination directory
            mode = os.stat(dest_path).st_mode
            self.assertTrue(mode & stat.S_IROTH != 0)  # file is world readable after the move

    def testMovedInvalidFile2(self):
        ''' Test to ensure that when None is returned from get_destination_path,
        the logic catches this and moves the misnamed file '''
        original_split_files = utilities.split_files

        try:
            filename = 'thisFileClearlyDoesntFitTheBill2.junk'
            utilities.split_files = Mock(return_value=([filename], []))
            src_path = os.path.join(self.test_root_directory, filename)
            with open(src_path, 'w') as f:
                f.write('testing')
            os.utime(src_path, (time.time(), time.time() - config.age_limit - 0.1))  # make the file old enough
            utilities.ingest_spice_files()  # move the file
            dest_path = os.path.join(config.invalid_dir, filename)
            self.assertFalse(os.path.isfile(src_path))  # the file was moved out of the source directory
            self.assertTrue(os.path.isfile(dest_path))  # the file was moved into the destination directory
        except Exception as failure:
            print (traceback.format_exc())
            self.fail(type(failure))
        finally:
            utilities.split_files = original_split_files

    def testMoveAndChangePermissions(self):
        filename = 'test_move_and_change_permissions.txt'
        src_path = os.path.join(self.test_root_directory, filename)
        dest_path = os.path.join(self.dest_directory, filename)

        with open(src_path, 'w') as f:
            f.write('generic file contents')
        with open(dest_path, 'w') as f:
            f.write('generic file contents')

        utilities.move_copy_update_permissions(src_path, dest_path, stat.S_IWOTH, True)
        self.assertFalse(os.path.isfile(src_path))  # the file was moved out of the source directory
        self.assertTrue(os.path.isfile(dest_path))  # the file was moved into the destination directory

    def testMoveAndChangePermissionsDestNotExists(self):
        filename = 'test_move_and_change_permissions.txt'
        src_path = os.path.join(self.test_root_directory, filename)
        dest_path = os.path.join(self.dest_directory, filename)

        with open(src_path, 'w') as f:
            f.write('generic file contents')

        self.assertFalse(os.path.isfile(dest_path))
        utilities.move_copy_update_permissions(src_path, dest_path, stat.S_IWOTH, True)
        # src_file moved to dest_file and made readable

        mode = os.stat(dest_path).st_mode
        self.assertTrue(mode & stat.S_IWOTH != 0)  # file is world writeable after the move

    def testMoveAndChangePermissionsException(self):
        filename = 'test_move_and_change_permissions.txt'
        src_path = os.path.join(self.test_root_directory, filename)
        dest_path = os.path.join(self.dest_directory, filename)

        with open(src_path, 'w') as f:
            f.write('generic file contents')
        with open(dest_path, 'w') as f:
            f.write('raise exception for file with same name but different contents')

        with self.assertRaises(Exception) as context:
            utilities.move_copy_update_permissions(src_path, dest_path, stat.S_IWOTH, True)
