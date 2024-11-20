import smtplib
import unittest
import os
import time
from ingest_l0_files import utilities, config
from shutil import rmtree
from mock import patch
from tests.maven_test_utilities import file_system
from tests.maven_test_utilities import mail_utilities
from maven_utilities import file_pattern, maven_config
from maven_utilities.utilities import file_is_old_enough
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'


class IngestL0FilesBehaviorsUnittest(unittest.TestCase):

    def setUp(self):
            # Remove ability to send emails
        smtplib.SMTP = mail_utilities.DummySMTP
        # create test directories and make sure it's clean
        self.test_directory = file_system.get_temp_root_dir()
        rmtree(self.test_directory)
        self.assertFalse(os.path.isdir(self.test_directory))

        self.invalid_dir_original = config.invalid_dir
        config.invalid_dir = '%s/%s' % (self.test_directory, config.invalid_dir)
        self.src_directory = '%s/maven/poc' % self.test_directory
        self.dest_root_directory = '%s/maven/data/sci' % self.test_directory

        os.makedirs(config.invalid_dir)
        os.makedirs(self.src_directory)
        os.makedirs(self.dest_root_directory)

        # Create test files
        source_filenames = []
        self.create_source_file('mvn_ngi_grp_l0_20121121.dat', 'these are the contents of the file', source_filenames, False)
        self.create_source_file('mvn_iuv_grp_l0_20121121.dat', 'these are the contents of the file', source_filenames, False)
        self.source_filenames = source_filenames
        # create destination directories
        for fn in self.source_filenames:
            inst = utilities.get_instrument_name(fn)
            dest_directory = '%s/%s/l0' % (self.dest_root_directory, inst)
            os.makedirs(dest_directory)
            assert os.path.isdir(dest_directory)

    def tearDown(self):
        config.invalid_dir = self.invalid_dir_original
        rmtree(self.test_directory)
        self.assertFalse(os.path.isdir(self.test_directory))

    def testMovingFiles(self):
        # confirm the files are old enough
        for fn in self.source_filenames:
            path = os.path.join(self.src_directory, fn)
            assert os.path.isfile(path)
            assert file_is_old_enough(path, config.age_limit)

        # run the ingest code that should move files to the correct destination directory
        utilities.move_files(self.src_directory, self.dest_root_directory)

        # check to make sure dest files are in their appropriate dest directory
        assert utilities.filename_exists_in_destination_directory(self.source_filenames[0], "%s/ngi/l0/" % (self.dest_root_directory))
        assert utilities.filename_exists_in_destination_directory(self.source_filenames[1], "%s/iuv/l0/" % (self.dest_root_directory))
        # check to make sure dest files are not in the other dest directory
        assert not utilities.filename_exists_in_destination_directory(self.source_filenames[0], "%s/iuv/l0/" % (self.dest_root_directory))
        assert not utilities.filename_exists_in_destination_directory(self.source_filenames[1], "%s/ngi/l0/" % (self.dest_root_directory))
        # check to make sure the source files are no longer in the source directory
        assert not os.path.isfile("%s/%s" % (self.src_directory, self.source_filenames[0]))
        assert not os.path.isfile("%s/%s" % (self.src_directory, self.source_filenames[1]))
    
    def testMovingFilesException(self):
        exception_src_directory = os.path.join(config.invalid_dir, 'src')
        with open(exception_src_directory, 'w') as f:
            f.write('testing')
        self.assertTrue(os.path.isfile(exception_src_directory))
        exception_dest_root_directory = os.path.join(config.invalid_dir, 'dest')
        self.assertFalse(os.path.isfile(exception_dest_root_directory))
        utilities.move_files(exception_src_directory, exception_dest_root_directory)
        self.assertTrue(os.path.isfile(exception_src_directory))  # the file was not moved out of the source directory
        self.assertFalse(os.path.isfile(exception_dest_root_directory))  # the file was not moved into the destination directory

    def testNotMovingYoungFiles(self):
        young_filenames = []
        self.create_source_file('mvn_ngi_grp_l0_20131121.dat', 'these are the contents of the file', young_filenames, True)
        self.create_source_file('mvn_iuv_grp_l0_20131121.dat', 'these are the contents of the file', young_filenames, True)

        for fn in young_filenames:
            path = os.path.join(self.src_directory, fn)
            assert os.path.isfile(path)
            # confirm files are not old enough
            assert not file_is_old_enough(path, config.age_limit)

        utilities.move_files(self.src_directory, self.dest_root_directory)

        # check to make sure young files are not in destination directories
        assert not utilities.filename_exists_in_destination_directory(young_filenames[0], "%s/ngi/l0" % (self.dest_root_directory))
        assert not utilities.filename_exists_in_destination_directory(young_filenames[1], "%s/iuv/l0" % (self.dest_root_directory))
        # check to make sure young files remain in the source directory
        assert os.path.isfile("%s/%s" % (self.src_directory, young_filenames[0]))
        assert os.path.isfile("%s/%s" % (self.src_directory, young_filenames[1]))

    def testMovingFilesWithOddNames(self):
        # TODO check if needed.
        with patch('maven_utilities.mail.send_email'):
            # create some files with odd filenames to make sure they remain in the source directory
            odd_filenames = []
            fns = ["mvn_foo_grp_l0_20121120.dat.not", "mvn_foo_grp_l0_20121120.vcdus", "mvn_foo_grp_20121120.dat", "totally_bogus_name"]
            for fn in fns:
                self.create_source_file(fn, 'odd contents', odd_filenames, False)

            utilities.move_files(self.src_directory, self.dest_root_directory)

            # test to make sure the odd files remain in the source directory and do not appear in any dest directory
            for fn in odd_filenames:
                path = os.path.join(config.invalid_dir, fn)
                assert os.path.isfile(path)
                assert not os.path.isfile("%s/iuv/l0/%s" % (self.dest_root_directory, fn))
                assert not os.path.isfile("%s/ngi/l0/%s" % (self.dest_root_directory, fn))
                assert not os.path.isfile("%s/foo/l0/%s" % (self.dest_root_directory, fn))

    def testMovingFilesWithMatchingFileInDestDir(self):
        # set up duplicate files in the dest directory
        for fn in self.source_filenames:
            self.create_dest_file(fn, 'these are the new contents of the file', '001')

        utilities.move_files(self.src_directory, self.dest_root_directory)

        # check to make sure there are two versions of the files and max version is 2
        dest_fns = []
        for _, _, files in os.walk(self.dest_root_directory):
            for f in files:
                m = file_pattern.matches_on_group([maven_config.l0_regex], f, [(file_pattern.general_version_group, file_pattern.not_empty_group_regex)])
                if m is not None:
                    dest_fns.append(f)
        for fn in self.source_filenames:
            matching_dest_fns = [dest_fn for dest_fn in dest_fns if self.filenames_match(fn, dest_fn)]
            self.assertEqual(2, len(matching_dest_fns))

            max_version = max([utilities.get_version(dest_fn) for dest_fn in matching_dest_fns])
            self.assertEqual(2, max_version)
            assert max_version == 2

    def testMovingIdenticalFilesWithMatchingFileInDestDir(self):
        # set up duplicate files in the dest directory
        for fn in self.source_filenames:
            self.create_dest_file(fn, 'these are the contents of the file', '001')

        utilities.move_files(self.src_directory, self.dest_root_directory)

        # check to make sure duplicate files are no longer in the source directory
        assert not os.path.isfile("%s/%s" % (self.src_directory, self.source_filenames[0]))
        assert not os.path.isfile("%s/%s" % (self.src_directory, self.source_filenames[1]))
        # check to make sure original files are still in the dest directory
        assert utilities.filename_exists_in_destination_directory(self.source_filenames[0], "%s/ngi/l0/" % (self.dest_root_directory))
        assert utilities.filename_exists_in_destination_directory(self.source_filenames[1], "%s/iuv/l0/" % (self.dest_root_directory))

    def testBogusFileInDestDirectory(self):
        # set up  files in the dest directory
        fn = 'mvn_ngi_grp_l0_12345678.dat'
        self.create_dest_file(fn, 'some bogus file contents', 'OOl')
        dest_fn = fn.replace('.dat', '_vOOl.dat')

        utilities.move_files(self.src_directory, self.dest_root_directory)

        # check that the source files were moved correctly, but invalid files are ignored left in the destination directory
        assert utilities.filename_exists_in_destination_directory(self.source_filenames[0], "%s/ngi/l0/" % (self.dest_root_directory))
        assert utilities.filename_exists_in_destination_directory(self.source_filenames[1], "%s/iuv/l0/" % (self.dest_root_directory))
        assert not utilities.filename_exists_in_destination_directory(fn, "%s/ngi/l0/" % (self.dest_root_directory))
        assert os.path.isfile("%s/ngi/l0/%s" % (self.dest_root_directory, dest_fn))

    def filenames_match(self, fn1, fn2):
        """ Returns True if the base of the two filenames jive. """
        m_f1 = file_pattern.extract_parts([maven_config.l0_regex], fn1, [maven_config.l0_base_group])
        assert m_f1 is not None
        m_f2 = file_pattern.extract_parts([maven_config.l0_regex], fn2, [maven_config.l0_base_group])
        assert m_f2 is not None
        f1_name, = m_f1.values()
        f2_name, = m_f2.values()
        return f1_name == f2_name

    def create_dest_file(self, fn, contents, version):
        """ Creates a new file in the destination directory

        fn: the filename as a string
        contents: the contents of the file as a string
        version: specifies the version of the destination file to make"""

        # create a file in the destination directory
        inst = utilities.get_instrument_name(fn)
        dest_dir = '%s/%s/l0' % (self.dest_root_directory, inst)
        dest_fn = fn.replace('.dat', '_v%s.dat' % version)
        path = os.path.join(dest_dir, dest_fn)
        f = open(path, 'w')
        f.write(contents)
        f.close()
        assert os.path.isfile(path)
        os.utime(path, (time.time(), time.time() - config.age_limit - 0.1))

    def create_source_file(self, fn, contents, array, young):
        """ creates a new file in the source directory
        and appends it to the provided array

        if the file type is listed as 'young',
        the file will not be aged artificially

        fn: the filename as a string
        contents: the file contents as a string
        array: the array to append the filename to.  Used for tracking files
        young: True if the file is to keep its creation time
                False if the file is to be artificially aged
        """

        # create a file in the source directory
        path = os.path.join(self.src_directory, fn)
        f = open(path, 'w')
        f.write(contents)
        f.close()
        if not young:
            os.utime(path, (time.time(), time.time() - config.age_limit - 0.1))
        array.append(os.path.basename(path))
        assert os.path.isfile(path)
