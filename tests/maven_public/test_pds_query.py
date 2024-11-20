'''
Created on June 27, 2019

@author: bharter
'''
import unittest
import os
import urllib
from shutil import rmtree
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from maven_public import utilities, config


class TestMavenPDSQuery(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parse_file_name(self):
        file_tuple = utilities.PdsQueryMetadata._parse_file_name("mvn_euv_l2_20190327_v01_r02")
        self.assertEqual(file_tuple, ("mvn_euv_l2_20190327", (1,2)))
        
    def test_each_url_valid(self):
        # If any of the following websites do not exist, it will throw an error

        for data_url in config.ppi_data_urls:
            url = os.path.join(config.ppi_search_url, data_url)
            page = urllib.request.urlopen(url)

        for data_url in config.atmos_data_urls:
            url = os.path.join(config.atmos_search_url, data_url)
            page = urllib.request.urlopen(url)

    def test_walk_through_atmos_pds(self):
        results = utilities.walk_through_pds(os.path.join(config.atmos_search_url, "iuvs-kp_bundle/kp/iuvs"))
        self.assertTrue(len(results) > 100)

    def test_walk_through_ppi_pds(self):
        results = utilities.walk_through_pds(config.ppi_search_url + "maven.euv.calibrated:data.bands%22", ppi=True)
        print(results)
        self.assertTrue(len(results) > 100)