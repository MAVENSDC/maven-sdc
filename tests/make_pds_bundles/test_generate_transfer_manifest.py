'''
Created on Oct 27, 2017

@author: bstaley
'''
import unittest
from make_pds_bundles import make_pds_bundles, config

class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testIuvLidGeneration(self):
        results = make_pds_bundles.generate_transfer_manifest(config.instrument_dictionary['iuv'],
                                                              ['mvn_iuv_l1b_star-orbit05236-muv_20170612T082315_v07_r01.fits.gz'])
        
        self.assertEqual(1, len(results))
        self.assertEqual('mvn_iuv_l1b_star-orbit05236-muv_20170612T082315_v07_r01.fits', results[0][1])
        
        lid_parts = results[0][0].split(':')
        self.checkLidParts(lid_parts, 'maven.iuvs.calibrated', 'calibration', 7, 1)
       
    
    def checkLidParts(self, parts, truth_lid, truth_collection, truth_ver, truth_rev):
        self.assertEqual(7, len(parts))
        self.assertEqual('urn', parts[0])
        self.assertEqual('nasa', parts[1])
        self.assertEqual('pds', parts[2])
        self.assertEqual(truth_lid, parts[3])
        self.assertEqual(truth_collection, parts[4])
        self.assertEqual('{}.{}'.format(truth_ver, truth_rev), parts[6])
        
        
