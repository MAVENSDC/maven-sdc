'''
Unit tests for the progress classes and progress handler classes
Created on Jun 18, 2015

@author: bstaley
'''
from io import StringIO
import smtplib
import unittest
import os
from maven_utilities import constants, progress

os.environ[constants.python_env] = 'testing'
from tests.maven_test_utilities import mail_utilities


class TestProgress(unittest.TestCase):

    def setUp(self):
        self.test_files = ['a.foo',
                           'b.foo',
                           'c.foo',
                           'd.foo']
        # Hijack SMTP
        self.mail_capture = mail_utilities.DummySMTP
        smtplib.SMTP = self.mail_capture

    def tearDown(self):
        self.mail_capture.messages = []  # clear any emails from prior tests
        
    def testCadenceHander(self):
        '''Test the ability to add a handler at a specified cadence'''
        prog = progress.Progress(self.test_files)
        test_handler = TestProgress.TestHandler(4)
        prog.add_handler(test_handler, .25)
        for f in self.test_files:
            prog.complete_unit(f)
        self.assertTrue(test_handler.expected_num_fires == len(test_handler.num_fires))

    def testEveryHandler(self):
        '''Test the ability to add a handler for every update'''
        prog = progress.Progress(self.test_files)
        test_handler = TestProgress.TestHandler(len(self.test_files))
        prog.add_handler(test_handler, None)
        for f in self.test_files:
            prog.complete_unit(f)
        self.assertTrue(test_handler.expected_num_fires == len(test_handler.num_fires))

    def testStandardHandler(self):
        '''Test the ability to fire the standard output hander'''
        test_prefix = 'cowabunga'
        prog = progress.Progress(self.test_files)
        output = StringIO()
        prog.add_handler(progress.StandardProgressHandler(prefix=test_prefix, output=output), None)
        for f in self.test_files:
            prog.complete_unit(f)
        self.assertEqual(len(self.test_files), output.getvalue().count(test_prefix))
        
    def testCompletePercentage(self):
        '''Test method used to get COMPLETED %'''
        prog = progress.Progress(self.test_files)
        complete_per = prog.get_complete_percentage()
        self.assertEqual(0.0, complete_per)

        prog = progress.Progress([])
        complete_per = prog.get_complete_percentage()
        self.assertEqual(1.0, complete_per)
    
    def testErrorPercentage(self):
        '''Test method used to get ERROR %'''
        prog = progress.Progress(self.test_files)
        complete_per = prog.get_error_percentage()
        self.assertEqual(0.0, complete_per)

        prog = progress.Progress([])
        complete_per = prog.get_error_percentage()
        self.assertEqual(1.0, complete_per)

    def testTotalPercentage(self):
        '''Test method used to get total (everything but IN_PROGRESS) %'''
        prog = progress.Progress(self.test_files)
        complete_per = prog.get_total_percentage()
        self.assertEqual(0.0, complete_per)

        prog = progress.Progress([])
        complete_per = prog.get_total_percentage()
        self.assertEqual(1.0, complete_per)
    
    def testErrorUnit(self):
        '''Marks a unit as ERROR'''
        prog = progress.Progress(self.test_files)
        for f in self.test_files:
            prog.error_unit(f)
        self.assertEqual(str(prog.status), "{'a.foo': -1, 'b.foo': -1, 'c.foo': -1, 'd.foo': -1}")

    def testEmailHandler(self):
        '''Test the ability to fire the email output hander'''
        test_prefix = 'cowabunga'
        prog = progress.Progress(self.test_files)
        prog.add_handler(progress.EmailProgressHandler(prefix=test_prefix), None)
        for f in self.test_files:
            prog.complete_unit(f)
        self.assertEqual(len(self.test_files), len(self.mail_capture.messages))

    def testBarHandler(self):
        '''Test the ability to fire the bar output hander'''
        test_prefix = 'cowabunga'
        prog = progress.Progress(self.test_files)
        output = StringIO()
        prog.add_handler(progress.BarProgressHandler(prefix=test_prefix, output=output), None)
        for f in self.test_files:
            prog.complete_unit(f)
        self.assertEqual(len(self.test_files), output.getvalue().count(test_prefix))
        outputs = output.getvalue().split('\r')
        outputs = [o for o in outputs if len(o) > 1]  # remove trailing \r

        for o in outputs:
            self.assertEqual(1, o.count(test_prefix))
            num_done = o.count('#')
            num_todo = o.count('-')
            self.assertEqual(progress.BarProgressHandler.bar_len, num_done + num_todo)

    class TestHandler(progress.ProgressHandler):
        '''Test implementation of the ProgressHandler'''

        def __init__(self, expected_num_fires):
            super(TestProgress.TestHandler, self).__init__()
            self.expected_num_fires = expected_num_fires
            self.num_fires = []

        def handle(self, num_success, num_error, total):
            self.num_fires.append((num_success, num_error, total))
    
