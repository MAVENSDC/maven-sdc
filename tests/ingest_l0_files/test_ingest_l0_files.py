import unittest
import os
import smtplib
import stat
import time
from mock import patch, Mock
from maven_utilities import maven_config
from ingest_l0_files import utilities, config
from tests.maven_test_utilities import file_system
from tests.maven_test_utilities import mail_utilities
from shutil import rmtree
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'


class IngestL0FilesUnittest(unittest.TestCase):

    save_invalid_dir = config.invalid_dir

    misnamed_test_file = 'thisFileClearlyDoesntFitTheBill.junk'

    def setUp(self):
        # Remove ability to send emails
        dummy_email = mail_utilities.DummySMTP
        smtplib.SMTP = dummy_email
        self.test_root = file_system.get_temp_root_dir()
        self.src_directory = os.path.join(self.test_root, 'src')
        os.mkdir(self.src_directory)
        self.dest_root_directory = os.path.join(self.test_root, 'dest')
        self.dest_directory = self.dest_root_directory + '/foo/l0'
        os.makedirs(self.dest_directory)
        self.dest_directory = self.dest_root_directory + '/bar/l0'
        os.makedirs(self.dest_directory)
        config.invalid_dir = os.path.join(self.test_root, 'invalid')
        os.mkdir(config.invalid_dir)

    def tearDown(self):
        rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))
        config.invalid_dir = self.save_invalid_dir

    def testGetInstrumentName(self):
        '''utilities.get_instrument_name should return the correct instrument name.'''
        filename = 'mvn_foo_grp_l0_20121109_v001.dat'
        inst = utilities.get_instrument_name(filename)
        self.assertEqual(inst, 'foo')

    def testIsInstrumentFilename(self):
        '''utilities.is_instrument_filename should behave correctly'''
        filename = 'mvn_foo_grp_l0_20121109.dat'
        self.assertTrue(utilities.is_instrument_filename('foo', filename))
        self.assertFalse(utilities.is_instrument_filename('bar', filename))
        filename = 'mvn_ATLO_foo_grp_l0_20121109_v001.dat'
        self.assertFalse(utilities.is_instrument_filename('foo', filename))

    def testIncrementVersion(self):
        '''utilities.increment_version should return a name with the version incremented by one.'''
        filename = 'mvn_foo_grp_l0_20121109_v001.dat'
        incremented_filename = utilities.increment_version(filename)
        self.assertEqual(incremented_filename, 'mvn_foo_grp_l0_20121109_v002.dat')

    def testCmpVersions(self):
        '''utilities.cmp_versions should behave correctly.'''
        fn1 = 'mvn_foo_grp_l0_20121109_v001.dat'
        fn2 = 'mvn_foo_grp_l0_20121109_v002.dat'
        self.assertEqual(utilities.cmp_versions(fn1, fn2), -1)
        self.assertEqual(utilities.cmp_versions(fn1, fn1), 0)
        self.assertEqual(utilities.cmp_versions(fn2, fn1), 1)

    def testGetVersion(self):
        '''utilities.get_version should return the correct version.'''
        fn = 'mvn_foo_grp_l0_20121109_v001.dat'
        self.assertEqual(utilities.get_version(fn), 1)

    def testGetMatchingDestFilename(self):
        '''utilities.get_matching_dest_filename should return the matching file that has the greatest version.'''
        src_filename = 'mvn_foo_grp_l0_20121109.dat'
        dest_path = os.path.join(self.dest_directory, 'mvn_foo_grp_l0_20121109_v001.dat')
        f = open(dest_path, 'w')
        f.close()
        matching_dest_fn = utilities.get_matching_dest_filename(src_filename, self.dest_directory)
        self.assertEqual(matching_dest_fn, 'mvn_foo_grp_l0_20121109_v001.dat')
        # add another file to the destination directory
        dest_path = os.path.join(self.dest_directory, 'mvn_foo_grp_l0_20121109_v002.dat')
        f = open(dest_path, 'w')
        f.close()
        matching_dest_fn = utilities.get_matching_dest_filename(src_filename, self.dest_directory)
        self.assertEqual(matching_dest_fn, 'mvn_foo_grp_l0_20121109_v002.dat')

    def testFilenameExistsInDestinationDirectory(self):
        '''utilities.filename_exists_in_destination_directory should find names that exist and not find names that do not exist.  '''
        src_filename = 'mvn_foo_grp_l0_20121109.dat'
        dest_filename = 'mvn_foo_grp_l0_20121109_v001.dat'
        dest_path = os.path.join(self.dest_directory, dest_filename)
        f = open(dest_path, 'w')
        f.close()
        self.assertTrue(os.path.isfile(dest_path))
        self.assertTrue(utilities.filename_exists_in_destination_directory(src_filename, self.dest_directory))
        non_existant_filename = 'mvn_foo_grp_l0_20121225.dat'
        non_existant_path = os.path.join(self.dest_directory, non_existant_filename)
        self.assertFalse(utilities.filename_exists_in_destination_directory(non_existant_filename, self.dest_directory))

    def testMatchesSrcFilename(self):
        '''utilities.matches_src_filename should match the src filename or not.'''
        src_filename = 'mvn_foo_grp_l0_20121109.dat'
        dest_filename = 'mvn_foo_grp_l0_20121109_v001.dat'
        self.assertTrue(utilities.matches_src_filename(src_filename, dest_filename))
        dest_filename = 'mvn_foo_grp_l0_20121110_v001.dat'
        self.assertFalse(utilities.matches_src_filename(src_filename, dest_filename))


    def testListdirFiles(self):
        '''utilities.listdir_files should only return the files that are in a directory'''
        fn = os.path.join(self.dest_root_directory, 'test_file')
        with (open(fn, 'w')) as f:
            f.write('test content')
        dn = os.path.join(self.dest_root_directory, 'test_dir')
        os.mkdir(dn)
        filenames = utilities.listdir_files(self.dest_root_directory)
        self.assertTrue('test_file' in filenames)
        self.assertFalse('test_dir' in filenames)

    def testSplitFiles(self):
        '''utilities.split_files should return list of files that match l0_regex'''
        test_good_filenames = ['mvn_abc_abc_l0_12345678.dat']

        test_bad_filenames = ['mvn_abc_abc_l0_1234.nogood',
                              'mvn_abc_abc_missingl0_1234.']
        test_all_filenames = test_good_filenames + test_bad_filenames

        result_good_filenames, result_bad_filenames = utilities.split_files(maven_config.l0_regex, test_all_filenames)

        self.assertTrue(set(test_good_filenames) == set(result_good_filenames), 'The good filenames were not found')
        self.assertTrue(set(test_bad_filenames) == set(result_bad_filenames), 'The bad filenames were not found')

    def sendMailCheckForFailedFileInBodySideEffect(self, subject, message):
        self.assertTrue(self.misnamed_test_file in message)

    def testMovedInvalidFile(self):
        # TODO check if needed.
        with patch('maven_utilities.mail.send_email', new=Mock(side_effect=self.sendMailCheckForFailedFileInBodySideEffect)):
            src_path = os.path.join(self.src_directory, self.misnamed_test_file)
            with open(src_path, 'w') as f:
                f.write('testing')
            os.utime(src_path, (time.time(), time.time() - config.age_limit - 0.1))  # make the file old enough
            utilities.move_files(self.src_directory, self.dest_directory)  # move the file
            dest_path = os.path.join(config.invalid_dir, self.misnamed_test_file)
            self.assertFalse(os.path.isfile(src_path))  # the file was moved out of the source directory
            self.assertTrue(os.path.isfile(dest_path))  # the file was moved into the destination directory
            mode = os.stat(dest_path).st_mode
            self.assertTrue(mode & stat.S_IROTH != 0)  # file is world readable after the move
