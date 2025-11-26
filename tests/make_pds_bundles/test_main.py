"""
Created on Jun 11, 2015

@author: bstaley
"""
import os
import sys
import smtplib
import unittest
from shutil import rmtree
from tests.maven_test_utilities import file_system, log_handlers, db_utils, mail_utilities
from make_pds_bundles import config, make_pds_bundles_main
from make_pds_bundles.make_pds_bundles import direct_out_logger
from maven_ops_database.database import init_db
from maven_utilities import maven_log, constants
os.environ[constants.python_env] = 'testing'

# Setup in-memory database
init_db()


class TestMakePdsArchiveMain(unittest.TestCase):
    # Remove ability to send emails
    smtplib.SMTP = mail_utilities.DummySMTP

    test_handler = log_handlers.RecordHandler()

    maven_log.config_logging()

    def setUp(self):

        self.root_dir = file_system.get_temp_root_dir()
        direct_out_logger.addHandler(self.test_handler)
        os.makedirs(os.path.join(self.root_dir, 'mavenpro'))

    def tearDown(self):
        db_utils.delete_data()
        rmtree(self.root_dir)
        self.assertFalse(os.path.isdir(self.root_dir))
        direct_out_logger.removeHandler(self.test_handler)
        self.test_handler.clear()
        db_utils.delete_data()

    def testMainDumpConfig(self):
        sys.argv = ['TestMakePdsArchiveMain', '2015-01-01', '2015-02-01', self.root_dir, '-i', 'all', '-d', '-c']
        args = make_pds_bundles_main.parse_arguments(sys.argv[1:])
        make_pds_bundles_main.main(**vars(args))
        self.assertEqual(len(config.instrument_config), len(self.test_handler.records))
        for inst in config.instrument_config:
            self.assertTrue(any([config.instrument_config[inst].instrument in r.message for r in self.test_handler.records]), '%s failed' % inst)

    def testMainDumpLidConfig(self):
        test_instruments = ['swi', 'iuv', 'swe']
        sys.argv = ['TestMakePdsArchiveMain', '2015-01-01', '2015-02-01', self.root_dir, '-d', '-l', '-i']
        sys.argv.extend(test_instruments)
        args = make_pds_bundles_main.parse_arguments(sys.argv[1:])
        make_pds_bundles_main.main(**vars(args))
        self.assertEqual(len(test_instruments), len(self.test_handler.records))
        for inst in test_instruments:
            self.assertTrue(any([inst in r.message for r in self.test_handler.records]))
        
    def testMainOverride(self):
        sys.argv = ['TestMakePdsArchiveMain', '2015-01-01', '2015-02-01', self.root_dir, '-o']
        sys.argv.extend(['override.py'])
        args = make_pds_bundles_main.parse_arguments(sys.argv[1:])
        with self.assertRaises(AssertionError) as context:
            make_pds_bundles_main.main(**vars(args))
        self.assertEqual('override.py is not a file!', str(context.exception))
    
    def testMainReport(self):
        sys.argv = ['TestMakePdsArchiveMain', '2015-01-01', '2015-02-01', self.root_dir, '-r']
        args = make_pds_bundles_main.parse_arguments(sys.argv[1:])
        with self.assertRaises(TypeError):
            make_pds_bundles_main.main(**vars(args))        
