'''
Created on Jun 11, 2015

@author: tbussell
'''
import os
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
import mock
import unittest
from ingest_anc_files import ingest_anc_files_main


class IngestAncFilesMainTestCase(unittest.TestCase):
    input = ['skip', 'abcd']

    def test_parse_arguments(self):
        result = ingest_anc_files_main.parse_arguments(self.input[1:])
        self.assertEqual(self.input[1], result.src_dir)

    def ingest_side_effect(self, *args):
        self.assertEqual(self.input[1], args[0])

    def test_main(self):
        with mock.patch('ingest_anc_files.utilities.ingest_anc_files',
                        side_effect=self.ingest_side_effect):
            ingest_anc_files_main.main(self.input)
