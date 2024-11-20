# allow test to run in PyDev
import os
import stat
import time
import unittest
from shutil import rmtree
from mock import Mock, patch
from ingest_anc_files import utilities, config
from maven_utilities.utilities import file_is_old_enough
from tests.maven_test_utilities import file_system
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'


class IngestAncFilesUnittest(unittest.TestCase):

    save_invalid_dir = config.invalid_dir

    misnamed_test_file = 'thisFileClearlyDoesntFitTheBill.junk'

    def setUp(self):
        self.test_root = file_system.get_temp_root_dir()
        self.src_directory = os.path.join(self.test_root, 'src')
        os.mkdir(self.src_directory)
        self.dest_root_directory = os.path.join(self.test_root, 'dest')
        self.dest_directory = os.path.join(self.dest_root_directory, 'foo/anc')
        os.makedirs(self.dest_directory)
        self.dest_directory = os.path.join(self.dest_root_directory, 'bar/anc')
        os.makedirs(self.dest_directory)
        self.dest_eng_imu_directory = os.path.join(self.dest_directory, 'eng/imu')
        os.makedirs(self.dest_eng_imu_directory)
        self.dest_optg_directory = os.path.join(self.dest_directory, 'optg')
        os.makedirs(self.dest_optg_directory)
        self.dest_eng_eps_directory = os.path.join(self.dest_directory, 'eng/eps')
        os.makedirs(self.dest_eng_eps_directory)
        self.dest_dns_trk_directory = os.path.join(self.dest_directory, 'trk')
        os.makedirs(self.dest_dns_trk_directory)
        config.invalid_dir = os.path.join(self.test_root, 'invalid')
        os.makedirs(config.invalid_dir)

    def tearDown(self):
        rmtree(self.test_root)
        self.assertFalse(os.path.isdir(self.test_root))
        config.invalid_dir = self.save_invalid_dir

    def testFileIsOldEnough(self):
        '''ingest_anc_files.file_is_old_enough should behave correctly'''
        filename = 'fake_anc_file'
        path = os.path.join(self.src_directory, filename)
        f = open(path, 'w')
        f.close()
        self.assertFalse(file_is_old_enough(path, config.age_limit))
        os.utime(path, (time.time(), time.time() - config.age_limit - 0.1))  # make the file old enough
        self.assertTrue(file_is_old_enough(path, config.age_limit))

    def testGetSrcFileNames(self):
        '''ingest_anc_files.get_src_file_names should return a list of the filenames in the src directory'''
        filename = 'fake_anc_file1'
        path = os.path.join(self.src_directory, filename)
        f = open(path, 'w')
        f.close()
        filename = 'fake_anc_file2'
        path = os.path.join(self.src_directory, filename)
        f = open(path, 'w')
        f.close()
        fns = utilities.get_src_file_names(self.src_directory)
        self.assertTrue(isinstance(fns, list))
        self.assertEqual(len(fns), 2)
        self.assertTrue('fake_anc_file1' in fns)
        self.assertTrue('fake_anc_file2' in fns)

    def testMovedOptgFileIsWorldReadable(self):
        filename = 'optg_orb_01473-01522_rec_v1.txt'
        dest_path = os.path.join(self.dest_optg_directory, filename)
        self.moveFileAndTestIfWorldReadable(filename, dest_path)

    def testMovedEngEpsFileIsWorldReadable(self):
        filename = 'sci_anc_eps14_240_241.drf'
        dest_path = os.path.join(self.dest_eng_eps_directory, filename)
        self.moveFileAndTestIfWorldReadable(filename, dest_path)

    def testMovedEngImuFileIsWorldReadable(self):
        filename = 'mvn_imu15_010_014.txt'
        dest_path = os.path.join(self.dest_eng_imu_directory, filename)
        self.moveFileAndTestIfWorldReadable(filename, dest_path)

        filename = 'mvn_imu15_010_014.dat'
        dest_path = os.path.join(self.dest_eng_imu_directory, filename)
        self.moveFileAndTestIfWorldReadable(filename, dest_path)

    def testMovedDnsTrkFileIsWorldReadable(self):
        filename = '143170320SC202DSS35_noHdr.234'
        dest_path = os.path.join(self.dest_dns_trk_directory, filename)
        self.moveFileAndTestIfWorldReadable(filename, dest_path)

    def moveFileAndTestIfWorldReadable(self, filename, dest_path):
        with patch('maven_utilities.mail.send_exception_email', new=Mock(side_effect=self.sendMailFailUnitTest)):

            src_path = os.path.join(self.src_directory, filename)
            with open(src_path, 'w') as f:
                f.write('testing')
            mode = os.stat(src_path).st_mode
            mode = mode & ~stat.S_IROTH  # clear the world readable bit
            os.chmod(src_path, mode)
            mode = os.stat(src_path).st_mode
            self.assertTrue(mode & stat.S_IROTH == 0)  # source file is not world readable
            os.utime(src_path, (time.time(), time.time() - config.age_limit - 0.1))  # make the file old enough
            utilities.ingest_anc_files(self.src_directory, self.dest_directory)  # move the file
            self.assertFalse(os.path.isfile(src_path))  # the file was moved out of the source directory
            self.assertTrue(os.path.isfile(dest_path))  # the file was moved into the destination directory
            mode = os.stat(dest_path).st_mode
            self.assertTrue(mode & stat.S_IROTH != 0)  # file is world readable after the move

    def testSplitFiles(self):
        test_good_filenames = ['sci_anc_eps14_240_241.drf',
                               #'optg_anything_should_work_after_optg.txt',
                               'mvn_rec_123456_123456_la_v20.sff',
                               'mvn_aaa15_999_999.dat']

        test_bad_filenames = ['wont_match_anthything',
                              'mvn_nope_999_999.dat']

        test_all_filenames = test_good_filenames + test_bad_filenames

        result_good_filenames, result_bad_filenames = utilities.split_files(test_all_filenames)

        self.assertEqual(set(test_good_filenames), set(result_good_filenames))
        self.assertEqual(set(test_bad_filenames), set(result_bad_filenames))

    def sendMailFailUnitTest(self, subject, message, exception, sender_username='tester@test'):
        self.fail("Mail %s wasn't expected" % subject)

    def sendMailCheckForFailedFileInBodySideEffect(self, subject, message, sender_username='tester@test'):
        self.assertTrue(self.misnamed_test_file in message)

    def testMovedInvalidFile(self):

        # TODO i think this can go away or be replaced
        with patch('maven_utilities.mail.send_email', new=Mock(side_effect=self.sendMailCheckForFailedFileInBodySideEffect)):
            src_path = os.path.join(self.src_directory, self.misnamed_test_file)
            with open(src_path, 'w') as f:
                f.write('testing')
            os.utime(src_path, (time.time(), time.time() - config.age_limit - 0.1))  # make the file old enough
            utilities.ingest_anc_files(self.src_directory, self.dest_directory)  # move the file
            dest_path = os.path.join(config.invalid_dir, self.misnamed_test_file)
            self.assertFalse(os.path.isfile(src_path))  # the file was moved out of the source directory
            self.assertTrue(os.path.isfile(dest_path))  # the file was moved into the destination directory
            mode = os.stat(dest_path).st_mode
            self.assertTrue(mode & stat.S_IROTH != 0)  # file is world readable after the move
    
    def testNoPatternMatchException(self):         
        # raise ingest_anc_files exception
        tmp_dir = 'path/to/nowhere/'
        src_path = os.path.join(tmp_dir, self.misnamed_test_file)
        self.assertRaises(BaseException, utilities.ingest_anc_files(src_path, self.dest_directory))
        
        # raise destination_path exception when the file does not match patterns
        with self.assertRaises(Exception):
            utilities.get_destination_path(self.misnamed_test_file, self.dest_directory)

    def testRemoveDropboxFile(self):
        # self.src_directory, self.dest_directory
        with patch('maven_utilities.mail.send_exception_email', new=Mock(side_effect=self.sendMailFailUnitTest)):
            fn = 'sci_anc_eps14_240_241.drf'
            src_path = os.path.join(self.src_directory, fn)
            with open(src_path, 'w') as f:
                f.write('testing')

            dest_path = os.path.join(self.dest_eng_eps_directory, fn)
            self.assertFalse(os.path.isfile(dest_path))
            with open(dest_path, 'w') as f:
                f.write('testing')
            self.assertTrue(os.path.isfile(dest_path))
            os.utime(src_path, (time.time(), time.time() - config.age_limit - 0.1))  # make the file old enough
            utilities.ingest_anc_files(self.src_directory, self.dest_directory)

    def testRemoveDropboxFileException(self):
        with patch('maven_utilities.mail.send_exception_email', new=Mock(side_effect=self.sendMailFailUnitTest)):
            fn = 'sci_anc_eps14_240_241.drf'
            src_path = os.path.join(self.src_directory, fn)
            with open(src_path, 'w') as f:
                f.write('testing')

            # testing exception (file exists with different contents)
            dest_path = os.path.join(self.dest_eng_eps_directory, fn)
            with open(dest_path, 'w') as f:
                f.write('raise exception for file with same name but different contents')
            os.utime(src_path, (time.time(), time.time() - config.age_limit - 0.1))  # make the file old enough
            self.assertRaises(Exception, utilities.ingest_anc_files(self.src_directory, self.dest_directory))
