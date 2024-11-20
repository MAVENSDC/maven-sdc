'''
Created on Jul 21, 2015
Unit test for the maven_config file pattern module
@author: bstaley
'''

import unittest
import re
import os
from maven_utilities import constants, anc_config, file_pattern, maven_config
os.environ[constants.python_env] = 'testing'


class TestMavenFilePattern(unittest.TestCase):

    def runMatchesCheck(self, good_names_list, pattern_regex_list, parts_list, group_regex_list):

        for f in good_names_list:
            self.assertTrue(file_pattern.matches(pattern_regex_list,
                                                 f), '{} did not match!'.format(f))
            self.assertTrue(file_pattern.matches_on_group(pattern_regex_list,
                                                          f,
                                                          group_regexes=group_regex_list), '{} did not match!'.format(f))
            self.assertIsNotNone(file_pattern.extract_parts(pattern_regex_list,
                                                            f,
                                                            parts_list).values(), '{} pattern returned None!'.format(f))
        self.assertFalse(file_pattern.matches(pattern_regex_list, ''))

    def testRootVerRevRegex(self):
        # Random sampling of real science files found by find . -name \*20150101\* | sort | uniq -w 7
        test_good_names = ['mvn_euv_l2_bands_20150101_v01_r00.cdf',
                           'mvn_euv_l2b_orbit_merged_v01_r01.sav'
                           'mvn_iuv_all_l0_20150101_v001.dat',
                           'mvn_kp_insitu_20150101_v00_r00.tab',
                           'mvn_lpw_l0b_act_20150101_v01_r01.cdf'
                           'mvn_lpw_ql_20150101.png',
                           'mvn_mag_all_l0_20150101_v001.dat',
                           'mvn_mag_ql_2015d001pl_20150101_v00_r02.sts',
                           'mvn_ngi_l1a_raw-hk-014496_20150101T015314_v01_r01.csv',
                           'mvn_pfp_all_l0_20150101_v001.dat',
                           'mvn_sep_anc_20150101_v01_r00.cdf',
                           'mvn_sep_l2_anc_20150101_v02_r00.cdf',
                           'mvn_sta_l2_2a-hkp_20150101_v00_r00.cdf',
                           'mvn_swe_l2_arc3d_20150101_v01_r01.cdf',
                           'mvn_swi_l2_coarsearc3d_20150101_v00_r00.cdf']

        self.runMatchesCheck(test_good_names,
                             [maven_config.root_verrev_ext_regex],
                             [file_pattern.general_root_group,
                              file_pattern.general_version_group,
                              file_pattern.general_revision_group,
                              file_pattern.general_extension_group],
                             [(file_pattern.general_extension_group, file_pattern.not_empty_group_regex)])

    def testAncillaryRegex(self):
        test_good_names = ['sci_anc_eps13_337_338.drf',
                           'sci_anc_gnc13_337_338.drf',
                           'sci_anc_ngms13_337_338.drf',
                           'sci_anc_pf13_337_338.drf',
                           'sci_anc_rs13_337_338.drf',
                           # 'sci_anc_sasm1_140709_140711_v03.drf',
                           'sci_anc_usm113_337_338.drf',
                           'sci_anc_sasm2_15_265_265.drf',
                           'sci_anc_usm5_15_265_265.drf',
                           ]

        self.runMatchesCheck(test_good_names,
                             [anc_config.ancillary_regex],
                             [anc_config.anc_base_group,
                              anc_config.anc_product_group,
                              anc_config.anc_yy_start_group,
                              anc_config.anc_doy_start_group,
                              anc_config.anc_doy_end_group,
                              file_pattern.general_extension_group],
                             [(file_pattern.general_extension_group, file_pattern.not_empty_group_regex)])

    def testAncillaryEngRegex(self):

        test_good_names = ['mvn_imu14_322_329.txt',
                           'mvn_imu15_002_003.txt',
                           'mvn_pte14_316_318.drf',
                           'mvn_pte15_220_222.drf',
                           ]

        self.runMatchesCheck(test_good_names,
                             [anc_config.ancillary_eng_regex],
                             [anc_config.anc_base_group,
                              anc_config.anc_product_group,
                              anc_config.anc_yy_start_group,
                              anc_config.anc_doy_start_group,
                              anc_config.anc_doy_end_group,
                              file_pattern.general_extension_group],
                             [(file_pattern.general_extension_group, file_pattern.not_empty_group_regex)])

    def testAncillaryMatchOne(self):

        test_good_names = ['142560200SC202DSS34_noHdr.234',
                           'anc_sci_svt_2015_215.drf',
                           'anc_sci_svt.drf',
                           # 'eps_mag1_140706_140707_v03.drf',
                           # 'maven_orb.bsp',
                           # 'maven_orb_rec_140922_150101_v1.bsp',
                           # 2 'mvn_anc_trk_151_152.tgz',
                           'mvn_app_pred_131210_131210_ISON_v01.bc'
                           'mvn_app_rec_141009_141010_v03.bc',
                           # 'mvn_app_rec_141013_141014_v03_manual.bc',
                           'mvn_app_rec_141016_141023_h00f_v03.bc',
                           'mvn_app_rec_150803_150803_scisvt_v01.bc',
                           'mvn_app_red_141222_v01.bc',
                           'mvn_app_rel_141010_141012_v01.bc',
                           'mvn_app_tst_131203_131204_MAVENGDS611ATP_v01.bc',
                           'mvn_app_tst_141229_141229_svt13128_v01.bc',
                           # 10 'mvn_ATLO_iuv_all_l0_20121213.bc',
                           # 'mvn_imu.dat.txt',
                           'mvn_iuv_all_l0_20131204_v001.bc',
                           'mvn_rec_131118_131118_v02.sff',
                           'mvn_rec_131120_131120_desat_v01.sff',
                           'mvn_rec_131210_131210_zero2noon_v1.sff',
                           'mvn_rec_140809_140809_updated_v03.sff',
                           'mvn_rec_141016_141023_updatedh00f_v03.sff',
                           # 4 'mvn_rec_att_15_215_0904_2258_SVT_SCI_app.bc',
                           # 4 'mvn_rec_att_deep_dip_svt_2013_128_app.bc',
                           'MVN_SCLKSCET.00000.tsc',
                           'mvn_sc_pred_131203_131203_tcm1_v01.bc',
                           'mvn_sc_pred_150417_150423_deep_dip_two_b1_v001.bc',
                           'mvn_sc_pred_150515_150519_vm023a_stellar_occ_v01.bc',
                           # ?? 'mvn_sc_pred_150801_150802_iuvs_stellar_occ_4_v01.r04.bc',
                           # ?? 'mvn_sc_pred_20141209_20141220_vm002week2_v01.bc',
                           'mvn_sc_rec_131118_131118_v02.bc',
                           'mvn_sc_rec_131118_131119_launch_v01.bc',
                           'mvn_sc_rec_131118_131119_v02.bc',
                           'mvn_sc_rec_131210_131210_zero2noon_v01.bc',
                           'mvn_sc_rec_141002_141002_transsvt_v01.bc',
                           'mvn_sc_rec_141016_141023_h00f_v03.bc',
                           'mvn_sc_rec_150803_150803_scisvt_v01.bc',
                           # ?? 'mvn_sc_red_141124_v01.bc',
                           'mvn_sc_rel_131118_131124_v01.bc',
                           'mvn_sc_tst_131203_131204_MAVENGDS611ATP_v01.bc',
                           'mvn_sc_tst_141229_141229_svt13128_v01.bc',
                           'mvn_tst_131118_131118_launchsvt_v01.sff',
                           'mvn_tst_140226_140226_v00.sff',
                           'mvn_tst_140921_140921_moisvt_v01.sff',
                           # THIS SHOULD PROBABLY MAKE IT IN 'naif0010.tls',
                           # 1 'optg_od029a_140922-151108_reference_v1.txt',
                           'optg_orb_00001-00001_00004_v1.txt',
                           'optg_orb_00001-00083_rec_v1.txt',
                           'optg_orb_00718-00721_00924_planning_v1.txt',
                           'optg_orb_141215-181110_reference_v1.txt',
                           # ?? 'optg_orb_150514-161101_reference_pcm28.txt',
                           # 'pck00009.tpc',
                           # 'rec_att_deep_dip_svt_2013_128_app.bc',
                           'sci_anc_eps14_286_287_manual.drf',
                           # 'sci_nom_svt_anc_eps.drf',
                           # 'sci_nom_svt_anc_eps_launch_324_0000.drf',
                           'spk_m_141027-151027_110526.bsp',
                           'spk_orb_141028-160101_130808_mvn.bsp',
                           'spk_orb_141028-160101_130808_mvn.bsp.lbl',
                           'spk_t_140921-141027_111129.bsp',
                           # 'SVT_SCI_APID_105_raw_sci_ancillary.pkt',
                           # 'TR_2013-06-20_123034_scisvt_IR_MK_EDIT.txt',
                           'trj_c_131118-140811_rec_v1.bsp',
                           # ?? no version 'trj_c_131118-141004_p00_cpwsr2_130328.bsp',
                           # ?? no version 'trj_c_od004a_131201-141002_131122_tcm1prelim.bsp',
                           'trj_o_od018a_140312-151108_reference_v1.bsp',
                           'trj_orb_00001-00001_00004_v1.bsp',
                           'trj_orb_00001-00083_rec_v1.bsp',
                           'trj_orb_00005-00008_00033_prm2prelim_v1.bsp',
                           'trj_orb_00718-00721_00924_planning_v1.bsp',
                           # 'trj_orb_00832-00833_reference_v1.bsp',
                           # 'trj_orb_141003-151108_reference_v1.bsp',
                           # 'trj_orb_150514-161101_reference_pcm28.bsp',
                           # 'trj_orb_150723-160110_reference_v1.bsp',
                           # 'trj_orb_151011-181101_151021_reference_v1.bsp',
                           # 'trj_orb_od025a_140527-151108_reference-prelim_v1.bsp',
                           # 'trj_orb_od029a_140708-151108_reference_v1.bsp',
                           # 'trj_orb_od030b_140708-140927_plm1-10.0_final_v1.bsp',
                           # 'trj_PCM-1_01203-01204_prelim.bsp',
                           # 'trj_PCM-1_01203-01204_prelim_v2.bsp',
                           # 'trj_PCM-1_01642-01643_final.bsp',
                           'mvn_pred_150831_150911_dd4_test_r03.sfp',
                           'mvn_pred_150801_150803_iuvs_stellar_occ_4_01707_v01.sfp',
                           'mvn_pred_150825_150901_vm033b_v01.r02.sfp'
                           ]

        ancillary_regex_list = [anc_config.ancillary_regex,
                                anc_config.ancillary_eng_regex,
                                anc_config.anc_sci_svt_regex,
                                anc_config.anc_app_regex,
                                anc_config.anc_iuv_all_regex,
                                anc_config.rec_app_regex,
                                anc_config.sc_regex,
                                anc_config.tst_regex,
                                anc_config.optg_orb_regex,
                                anc_config.optg_orb_regex_2,
                                anc_config.anc_eps_regex,
                                anc_config.anc_spk_regex,
                                anc_config.anc_tsc_regex,
                                anc_config.anc_trk_regex,
                                anc_config.anc_trj_rec_regex,
                                anc_config.anc_trj_pred_regex,
                                anc_config.anc_trj_oc_regex,
                                anc_config.anc_pred_sfp_regex,
                                ]

        # Assert we match 1 and only 1
        for next_good_name in test_good_names:
            matched = False
            for next_anc_regex in ancillary_regex_list:
                next_matched = next_anc_regex.match(next_good_name) is not None
                self.assertFalse(next_matched and matched, '%s matched more than 1 ancillary regex!' % next_good_name)
                matched = next_matched if next_matched else matched
            self.assertTrue(matched, "%s didn't match any ancillary regex!" % next_good_name)

    def testL0Regex(self):
        test_good_names = ['mvn_iuv_all_l0_20141101_v001.dat',
                           'mvn_mag_all_l0_20141101_v001.dat',
                           'mvn_pfp_all_l0_20141101_v001.dat'
                           ]

        self.runMatchesCheck(test_good_names,
                             [maven_config.l0_regex],
                             [file_pattern.general_basename_group,
                              maven_config.l0_base_group,
                              file_pattern.general_instrument_group,
                              maven_config.l0_grouping_group,
                              file_pattern.general_level_group,
                              file_pattern.general_year_group,
                              file_pattern.general_month_group,
                              file_pattern.general_day_group,
                              file_pattern.general_version_group,
                              file_pattern.general_extension_group],
                             [(file_pattern.general_extension_group, file_pattern.not_empty_group_regex)])

    def testQlRegex(self):
        test_good_names = ['mvn_lpw_ql_20141101.png',
                           'mvn_mag_ql_20141101.png',
                           'mvn_pfp_ql_20141101.png',
                           'mvn_sep_ql_20141101.png',
                           'mvn_sta_ql_20141101.png',
                           'mvn_swe_ql_20141101.png',
                           'mvn_swi_ql_20141101.png',
                           'mvn_pfp_l2_peri_20190504_009041.png'

                           ]

        self.runMatchesCheck(test_good_names,
                             [maven_config.ql_regex],
                             [file_pattern.general_instrument_group,
                              file_pattern.general_level_group,
                              file_pattern.general_year_group,
                              file_pattern.general_month_group,
                              file_pattern.general_day_group,
                              file_pattern.general_extension_group],
                             [(file_pattern.general_extension_group, file_pattern.not_empty_group_regex)])

    def testPlaybackRegex(self):
        test_good_names = ['pfp_test_playback.test2.42']

        self.runMatchesCheck(test_good_names,
                             [maven_config.playback_file_regex],
                             [file_pattern.general_instrument_group,
                              maven_config.pfp_playback_data_group,
                              file_pattern.general_description_group,
                              file_pattern.general_extension_group],
                             [(file_pattern.general_extension_group, file_pattern.not_empty_group_regex)])

    def testScienceRegex(self):
        test_good_names = ['mvn_euv_l2_bands_20141101_v01_r03.cdf',
                           'mvn_euv_l3_daily_20141101_v01_r05.cdf',
                           'mvn_lpw_l0b_act_20141101_v01_r01.cdf',
                           'mvn_lpw_l2_lpiv_20141101_v01_r01.cdf',
                           'mvn_mag_l2_2014305pc_20141101_v01_r01.sts',
                           'mvn_ngi_l1a_raw-hk-014060_20141101T235944_v01_r01.csv',
                           'mvn_ngi_l1b_cal-hk-014060_20141101T235944_v01_r01.csv',
                           'mvn_sep_l2_anc_20141101_v02_r00.cdf',
                           'mvn_sta_l2_2a-hkp_20141101_v00_r00.cdf',
                           'mvn_swe_l2_arc3d_20141101_v01_r02.cdf',
                           'mvn_swi_l2_coarsearc3d_20141101_v00_r06.cdf'
                           ]

        self.runMatchesCheck(test_good_names,
                             [maven_config.science_regex],
                             [file_pattern.general_instrument_group,
                              file_pattern.general_level_group,
                              file_pattern.general_description_group,
                              file_pattern.general_year_group,
                              file_pattern.general_month_group,
                              file_pattern.general_day_group,
                              file_pattern.general_hhmmss_group,
                              file_pattern.general_version_group,
                              file_pattern.general_revision_group,
                              file_pattern.general_extension_group,
                              file_pattern.general_gz_extension_group],
                             [(file_pattern.general_extension_group, file_pattern.not_empty_group_regex)])

    def testLabelRegex(self):
        test_good_names = ['mvn_acc_l3_pro-acc-p00289_20141122_v01_r01.xml',
                           'mvn_iuv_l1a_inbound-orbit00238-fuv_20141112T135825.xml',
                           'mvn_iuv_l1a_apoapse-orbit00236-fuv_20141112T025918.xml',
                           'mvn_iuv_l1a_inbound-orbit00240-ech_20141112T231101.xml',
                           'mvn_iuv_l1a_outlimb-orbit00236-fuv_20141112T021423.xml',
                           'mvn_iuv_l1a_APP2-orbit00254-mode0111-fuv_20141115T130546.xml',
                           'mvn_iuv_l1b_inbound-orbit00238-fuv_20141112T135825.xml',
                           'mvn_iuv_l1b_apoapse-orbit00236-fuv_20141112T025918.xml',
                           'mvn_iuv_l1b_inbound-orbit00240-ech_20141112T231101.xml',
                           'mvn_iuv_l1b_outlimb-orbit00236-fuv_20141112T021423.xml',
                           'mvn_iuv_l1b_APP2-orbit00254-mode0111-fuv_20141115T130546.xml',
                           'mvn_iuv_l1c_corona-orbit00238-fuv_20141112T112915.xml',
                           'mvn_iuv_l1c_apoapse-orbit00236_20141112T025918.xml',
                           'mvn_iuv_l1c_periapse-orbit00261_20141116T202309.xml',
                           'mvn_iuv_l2_periapse-orbit00261_20141116T202309.xml',
                           'mvn_mag_l2_2014305pc_20141101_v01_r01.xml',
                           'mvn_ngi_l1a_raw-hk-014060_20141101T235944_v01_r01.xml',
                           'mvn_ngi_l1b_cal-hk-014060_20141101T235944_v01_r01.xml',
                           'mvn_ngi_l2_csn-abund-14069_20141116T142750_v04_r03.xml',
                           'mvn_ngi_l3_res-den-14069_20141116T142750_v01_r02.xml'
                           ]

        self.runMatchesCheck(test_good_names,
                             [maven_config.label_regex],
                             [file_pattern.general_instrument_group,
                              file_pattern.general_level_group,
                              file_pattern.general_description_group,
                              file_pattern.general_year_group,
                              file_pattern.general_month_group,
                              file_pattern.general_day_group,
                              file_pattern.general_hhmmss_group,
                              file_pattern.general_version_revision_group,
                              file_pattern.general_version_group,
                              file_pattern.general_revision_group,
                              file_pattern.general_extension_group,
                              file_pattern.general_gz_extension_group],
                             [(file_pattern.general_extension_group, file_pattern.not_empty_group_regex)])

    def testKpRegex(self):
        test_good_names = ['mvn_kp_insitu_20141101_v00_r01.tab'
                           ]

        self.runMatchesCheck(test_good_names,
                             [maven_config.kp_regex],
                             [file_pattern.general_instrument_group,
                              file_pattern.general_level_group,
                              file_pattern.general_description_group,
                              file_pattern.general_year_group,
                              file_pattern.general_month_group,
                              file_pattern.general_day_group,
                              file_pattern.general_hhmmss_group,
                              file_pattern.general_version_group,
                              file_pattern.general_revision_group,
                              file_pattern.general_extension_group,
                              file_pattern.general_gz_extension_group],
                             [(file_pattern.general_extension_group, file_pattern.not_empty_group_regex)])

    def testSepAncRegex(self):
        test_good_names = ['mvn_sep_anc_20141101_v01_r00.cdf',
                           'mvn_sep_l2_pad_20151115_v02_r00.sav'
                           ]

        self.runMatchesCheck(test_good_names,
                             [maven_config.sep_anc_regex],
                             [file_pattern.general_instrument_group,
                              file_pattern.general_level_group,
                              file_pattern.general_description_group,
                              file_pattern.general_year_group,
                              file_pattern.general_month_group,
                              file_pattern.general_day_group,
                              file_pattern.general_hhmmss_group,
                              file_pattern.general_version_group,
                              file_pattern.general_revision_group,
                              file_pattern.general_extension_group],
                             [(file_pattern.general_extension_group, file_pattern.not_empty_group_regex)])

    def testEuvL2bRegex(self):
        test_good_names = ['mvn_euv_l2b_orbit_merged_v01_r00.sav',
                           ]

        self.runMatchesCheck(test_good_names,
                             [maven_config.euv_l2b_regex],
                             [file_pattern.general_instrument_group,
                              file_pattern.general_level_group,
                              file_pattern.general_description_group,
                              file_pattern.general_year_group,
                              file_pattern.general_month_group,
                              file_pattern.general_day_group,
                              file_pattern.general_hhmmss_group,
                              file_pattern.general_version_group,
                              file_pattern.general_revision_group,
                              file_pattern.general_extension_group],
                             [(file_pattern.general_extension_group, file_pattern.not_empty_group_regex)])

    def testSisRegex(self):
        test_good_names = ['ngims_pds_sis_v01_r05.pdf.gz',
                           'ngims_pds_sis_v01_r05.xml.gz']

        self.runMatchesCheck(test_good_names,
                             [maven_config.sis_regex],
                             [file_pattern.general_instrument_group,
                              file_pattern.general_level_group,
                              maven_config.meta_type_group,
                              file_pattern.general_version_group,
                              file_pattern.general_revision_group,
                              file_pattern.general_extension_group,
                              file_pattern.general_gz_extension_group
                              ],
                             [(file_pattern.general_extension_group, file_pattern.not_empty_group_regex)])

    def testExtractOrder(self):
        test_name = 'mvn_ngi_l1a_raw-hk-014060_20141101T235944_v02_r01.csv'

        a, b, c, d, e, f, g, h, i, j, k = file_pattern.extract_parts([maven_config.science_regex],
                                                                     test_name,
                                                                     [file_pattern.general_gz_extension_group,
                                                                      file_pattern.general_extension_group,
                                                                      file_pattern.general_revision_group,
                                                                      file_pattern.general_version_group,
                                                                      file_pattern.general_hhmmss_group,
                                                                      file_pattern.general_day_group,
                                                                      file_pattern.general_month_group,
                                                                      file_pattern.general_year_group,
                                                                      file_pattern.general_description_group,
                                                                      file_pattern.general_level_group,
                                                                      file_pattern.general_instrument_group]).values()

        self.assertIsNone(a)
        self.assertEqual('csv', b)
        self.assertEqual('01', c)
        self.assertEqual('02', d)
        self.assertEqual('T235944', e)
        self.assertEqual('01', f)
        self.assertEqual('11', g)
        self.assertEqual('2014', h)
        self.assertEqual('_raw-hk-014060', i)
        self.assertEqual('l1a', j)
        self.assertEqual('ngi', k)

    def testHhmmssTransformer(self):
        test_name = 'mvn_ngi_l1a_raw-hk-014060_20141101T235944_v01_r01.csv'
        expected_hh = 23
        expected_mm = 59
        expected_ss = 44

        (hh, mm, ss), = file_pattern.extract_parts([maven_config.science_regex],
                                                   test_name,
                                                   [file_pattern.general_hhmmss_group],
                                                   transforms={file_pattern.general_hhmmss_group: file_pattern.thhmmss_extractor}).values()

        self.assertEqual(expected_hh, hh)
        self.assertEqual(expected_mm, mm)
        self.assertEqual(expected_ss, ss)

        # test if there is no thhmmss in given valid filename
        test_name_no_date = 'mvn_ngi_l1a_raw-hk-014060_20141101_v01_r01.csv'
        (hh, mm, ss), = file_pattern.extract_parts([maven_config.science_regex],
                                                   test_name_no_date,
                                                   [file_pattern.general_hhmmss_group],
                                                   transforms={file_pattern.general_hhmmss_group: file_pattern.thhmmss_extractor}).values()
        self.assertIsNone(hh)
        self.assertIsNone(mm)
        self.assertIsNone(ss)

    def testRemoveUnderScore(self):
        test_underscore = 'test_filename_underscore.csv'
        self.assertEqual(test_underscore.replace('_', ''), file_pattern.remove_underscore_extractor(test_underscore))

    def testSafeInt(self):
        safe_int_0 = '31415'
        safe_int_1 = 926535
        not_safe_int = None

        self.assertEqual(31415, file_pattern.safe_int(safe_int_0))
        self.assertEqual(926535, file_pattern.safe_int(safe_int_1))
        self.assertIsNone(file_pattern.safe_int(not_safe_int))

    def testZeroToNoneTransformer(self):
        test_name = 'mvn_iuv_all_l0_20150101_v001.dat'  # no revision

        revision, = file_pattern.extract_parts([maven_config.root_verrev_ext_regex],
                                               test_name,
                                               [file_pattern.general_revision_group],
                                               transforms={file_pattern.general_revision_group: file_pattern.zero_len_to_none}).values()
        self.assertIsNone(revision)

        self.assertEqual(test_name, file_pattern.zero_len_to_none(test_name))
        self.assertIsNone(file_pattern.zero_len_to_none(''))

    def testMatchesOnGroup(self):
        test_name = 'mvn_ngi_l1a_raw-hk-014060_20141101T235944_v01_r01.csv'

        m = file_pattern.matches_on_group([maven_config.science_regex],
                                          test_name,
                                          [(file_pattern.general_extension_group, re.compile('csv'))])

        self.assertIsNotNone(m)

        m = file_pattern.matches_on_group([maven_config.science_regex],
                                          test_name,
                                          [(file_pattern.general_extension_group, re.compile('bws'))])
        self.assertIsNone(m)
