'''
Created on Sep 23, 2014

@author: Taylor Graham
'''
from monitoring import quicklook
import unittest
import shutil
import os
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from monitoring import config
from tests.maven_test_utilities import file_system
from subprocess import getstatusoutput


class TestQuicklook(unittest.TestCase):
    '''Unit tests for monitoring.quicklook
    '''
    @classmethod
    def setUpClass(cls):
        cls.base_test_dir = file_system.get_temp_root_dir()
        cls.inst_pkg_array = []

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.base_test_dir)

    def setUp(self):
        '''Setup test directories
        '''
        self.destination_dir = config.destination_dir
        self.source_dir = config.source_root
        
        self.public_destination_dir = config.public_destination_dir
        self.public_source_dir = config.public_source_root
        
        # create test directories
        config.destination_dir = self.base_test_dir + self.destination_dir
        config.source_root = self.base_test_dir + self.source_dir
        config.public_destination_dir = self.base_test_dir + self.public_destination_dir
        config.public_source_root = self.base_test_dir + self.public_source_dir

        inst = config.pf_source
        self.inst_pkg_array = inst.split(',')
        self.inst_pkg_array.append('iuv')
        
        #Create team site quiclook files
        for item in self.inst_pkg_array:
            path = config.source_root + item + '/ql/'
            os.makedirs(path)
            
            fn = 'mvn_%s_ql_11212013.png' % item
            fn = os.path.join(path, fn)
            self.createFile(fn, 'I dunno\n')

            fn = 'mvn_%s_ql_01012014.png' % item
            fn = os.path.join(path, fn)
            self.createFile(fn, 'Idunno\n')
        
        #Create public site quiclook files
        for item in self.inst_pkg_array:
            public_path = config.public_source_root + item + '/ql/'
            os.makedirs(public_path)
            
            fn = 'mvn_%s_ql_20150101.png' % item
            fn = os.path.join(public_path, fn)
            self.createFile(fn, 'I dont know\n')
            
            fn = 'mvn_%s_ql_20170102.png' % item
            fn = os.path.join(public_path, fn)
            self.createFile(fn, 'I am unaware of the specifics\n')

        os.makedirs(config.destination_dir)
        os.makedirs(config.public_destination_dir)

    def tearDown(self):
        '''Clean up of test directories
        '''
        config.destination_dir = self.destination_dir
        config.source_root = self.source_dir
        config.public_destination_dir = self.public_destination_dir
        config.public_source_root = self.public_source_dir
        
        s, _ = getstatusoutput('rm -fr %s/*' % self.base_test_dir)
        assert s == 0, 'rm -rf of test directory failed'

    def test_quicklook(self):
        quicklook.quicklook('iuv')
        quicklook.quicklook('pfp')
        for item in self.inst_pkg_array:
            
            # assert dest files exist
            fn = config.destination_dir + item + '.js'
            assert(os.path.isfile(fn))
            public_fn = config.public_destination_dir + item + '.js'
            assert(os.path.isfile(public_fn))
            
            # assert dest files have appropriate contents
            frmt_items = (self.base_test_dir, item, item, self.base_test_dir, item, item)
            filecontents = ','.join(
                ['[\"%s/maven/data/sci/%s/ql/mvn_%s_ql_01012014.png\"',
                 '"%s/maven/data/sci/%s/ql/mvn_%s_ql_11212013.png\"]']) % (frmt_items)

            with open('%s/%s.js' % (config.destination_dir, item), 'r') as f:
                filecontents_real = f.read()
                self.assertEqual(filecontents, filecontents_real)
                
            public_filecontents = ','.join(
                ['[\"%s/maven/public/sci/%s/ql/mvn_%s_ql_20150101.png\"',
                 '"%s/maven/public/sci/%s/ql/mvn_%s_ql_20170102.png\"]']) % (frmt_items)

            with open('%s/%s.js' % (config.public_destination_dir, item), 'r') as f:
                public_filecontents_real = f.read()
                self.assertEqual(public_filecontents, public_filecontents_real)

    def createFile(self, path, contents):
        '''Creates a new file
        path: the full pathname of the file location as a string
        contents: the file  contents as a string
        '''
        with open(path, 'w') as f:
            f.write(contents)
