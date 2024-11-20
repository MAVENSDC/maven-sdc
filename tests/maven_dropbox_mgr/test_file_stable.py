'''Tests of the get_files_of_stable_size function in maven_dropbox_mgr.utilities.

Created on Nov 17, 2014

@author: Kim Kokkonen
'''
import os
import stat
import threading
import time
import unittest
import random
from shutil import rmtree
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from maven_dropbox_mgr import config, utilities
from tests.maven_test_utilities.file_system import get_temp_root_dir
from maven_utilities.utilities import file_is_old_enough
from maven_utilities import time_utilities


class FileStableTestCase(unittest.TestCase):

    def setUp(self):
        self.root_source_directory = get_temp_root_dir()

    def tearDown(self):
        rmtree(self.root_source_directory)
        self.assertFalse(os.path.isdir(self.root_source_directory))

    def make_file(self, fn, size):
        with open(fn, 'w') as f:
            for i in range(size):
                f.write(chr((i % 26) + ord('a')))
        os.chmod(fn, stat.S_IXUSR + stat.S_IRUSR + stat.S_IWUSR)

    def test_immediately_stable(self):
        # use short times for testing
        config.age_limit = 0.0

        fn = os.path.join(self.root_source_directory, 'test_1')
        self.make_file(fn, 1)
        time.sleep(1.0)
        v = file_is_old_enough(fn, config.age_limit)
        self.assertTrue(v, 'File %s is not old enough?' % fn)
        v = utilities.file_is_open(fn)
        self.assertFalse(v, 'File %s is open?' % fn)
        v = utilities.file_is_stable(fn)
        self.assertTrue(v, 'File %s is not stable?' % fn)

    def grow_file(self, fn, secs):
        # use unbuffered writes
        with open(fn, 'ab', buffering=0) as f:
            time_secs = 0
            sleep_secs = 0.2
            while time_secs < secs:
                f.write('+'.encode())
                time.sleep(sleep_secs)
                time_secs += sleep_secs

    def test_eventually_stable(self):
        # use short times for testing
        config.age_limit = 1.0

        fn = os.path.join(self.root_source_directory, 'test_1')
        self.make_file(fn, 1)

        start_time = time_utilities.utc_now()

        # start a thread that makes the file grow for a while
        grow_secs = 2.0
        threading.Thread(target=self.grow_file, args=(fn, grow_secs)).start()

        # wait for stable or timeout
        time_secs = 0.0
        sleep_secs = 0.1
        while time_secs < 10.0 and not utilities.file_is_stable(fn):
            time.sleep(sleep_secs)
            time_secs += sleep_secs

        elapsed_time = (time_utilities.utc_now() - start_time).total_seconds()

        # elapsed time should be at least age_limit + grow_secs
        lower_limit = grow_secs + config.age_limit
        self.assertTrue(elapsed_time >= lower_limit,
                        'Found stable file before it was stable: %0.2f < %0.2f'
                        % (elapsed_time, lower_limit))

        # elapsed_time should be less than age_limit + grow_secs + slop
        upper_limit = lower_limit + 4.0
        self.assertTrue(elapsed_time < upper_limit,
                        "Didn't find stable file in a reasonable time: %0.2f > %0.2f"
                        % (elapsed_time, upper_limit))

    def grow_file_with_pauses(self, fn, secs):
        # use unbuffered writes
        with open(fn, 'ab', buffering=0) as f:
            time_secs = 0
            sleep_secs = 0.2
            while time_secs < secs:
                f.write('+'.encode())
                sleep_this_secs = sleep_secs
                if random.random() < 0.25:
                    # randomly 25% of the time,
                    # sleep for longer
                    utilities.logger.debug("long pause")
                    sleep_this_secs = 2.0
                time.sleep(sleep_this_secs)
                time_secs += sleep_this_secs
                f.write('-'.encode())

    def test_stable_with_pauses(self):
        # use short times for testing
        config.age_limit = 0.5

        fn = os.path.join(self.root_source_directory, 'test_1')
        self.make_file(fn, 1)

        start_time = time_utilities.utc_now()

        # start a thread that makes the file grow for a while
        grow_secs = 4.0
        threading.Thread(target=self.grow_file_with_pauses, args=(fn, grow_secs)).start()

        # wait for stable or timeout
        time_secs = 0.0
        sleep_secs = 0.1
        while time_secs < 10.0 and not utilities.file_is_stable(fn):
            time.sleep(sleep_secs)
            time_secs += sleep_secs

        elapsed_time = (time_utilities.utc_now() - start_time).total_seconds()
        utilities.logger.debug('elapsed time: %s' % elapsed_time)

        # elapsed time should be at least age_limit + grow_secs
        lower_limit = grow_secs + config.age_limit
        self.assertTrue(elapsed_time >= lower_limit,
                        'Found stable file before it was stable: %0.2f < %0.2f'
                        % (elapsed_time, lower_limit))

        # elapsed_time should be less than age_limit + grow_secs + slop based on long pause time
        upper_limit = lower_limit + 2.5
        self.assertTrue(elapsed_time < upper_limit,
                        "Didn't find stable file in a reasonable time: %0.2f > %0.2f"
                        % (elapsed_time, upper_limit))
