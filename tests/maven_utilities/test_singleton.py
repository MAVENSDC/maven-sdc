import os
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
import unittest
import logging
from time import sleep
from multiprocessing import Process, Event
from maven_utilities.singleton import SingleInstance
from threading import Thread


def logged_helper(name):
    tmp = logger.level
    logger.setLevel(logging.CRITICAL)  # we do not want to see the warning
    _ = SingleInstance(flavor_id=name)
    logger.setLevel(tmp)


def lock_helper(name, event):
    tmp = logger.level
    logger.setLevel(logging.CRITICAL)
    event.wait()
    _ = SingleInstance(flavor_id=name)
    sleep(1)
    logger.setLevel(tmp)


def start_processes(num_processes, results, event=None, helper=lock_helper,):
    processes = [Process(target=helper, args=("0", event,)) for _ in range(num_processes)]
    for p in processes:
        p.start()
        p.join()
        results.append(p.exitcode)

    '''Tests singleton.py functionality'''


class testSingleton(unittest.TestCase):

    def test_1(self):
        me = SingleInstance(flavor_id="test-1")
        del me  # now the lock should be removed
        assert True

    def test_2(self):
        p = Process(target=logged_helper, args=("test-2",))
        p.start()
        p.join()
        assert p.exitcode == 0, "%s != 0" % p.exitcode  # the called function should succeed

    def test_3(self):
        _ = SingleInstance(flavor_id="test-3")
        p = Process(target=logged_helper, args=("test-3",))
        p.start()
        p.join()
        assert p.exitcode != 0, "%s != 0 (2nd execution)" % p.exitcode  # the called function should fail because we already have another instance running
        # note, we return -1 but this translates to 255 meanwhile we'll consider that anything different from 0 is good
        p = Process(target=logged_helper, args=("test-3",))
        p.start()
        p.join()
        assert p.exitcode != 0, "%s != 0 (3rd execution)" % p.exitcode  # the called function should fail because we already have another instance running

    def test_same_start_time(self):
        event = Event()
        iterations = 5
        num_threads = 50
        num_processes = 1
        for _ in range(iterations):
            event.clear()
            results = []

            threads = [Thread(target=start_processes, args=(num_processes, results, event,)) for _ in range(num_threads)]
            for t in threads:
                t.start()

            sleep(1)
            event.set()

            for t in threads:
                t.join()

            self.assertEqual(1, results.count(0))


logger = logging.getLogger("tendo.singleton")
logger.addHandler(logging.StreamHandler())
