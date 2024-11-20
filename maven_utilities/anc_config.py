'''
Created on Dec 14, 2015

@author: bstaley
'''

import re
from maven_utilities import file_pattern
from maven_utilities.file_pattern import general_file_root_group, \
    general_extension_group
'''
Contains the MAVEN Ancillary mission-specific regexes from file_pattern
'''
# Ancillary group definitions
anc_base_group = 'anc_base'
anc_product_group = 'anc_product'
anc_description_group = 'anc_description'

anc_doy_start_group = 'anc_start_doy'
anc_doy_end_group = 'anc_end_doy'
anc_yy_start_group = 'anc_yy_start'
anc_yyyy_start_group = 'anc_yyyy_start'
anc_yyyy_end_group = 'anc_yyyy_end'

anc_label_ext_group = 'anc_label_ext'
anc_orb_start = 'anc_orb_start'
anc_orb_end = 'anc_orb_end'
anc_orb_predicted = 'anc_orb_predicted'

anc_rsr_subchannel = 'anc_rsr_subchannel'
anc_rsr_id = 'anc_rsr_id'

# Ancillary pattern
ancillary_pattern = (r'^(?P<{0}>sci_anc)_'
                     '(?P<{1}>[a-zA-Z0-9]*)'
                     '(?:|_20|_)'
                     '(?P<{2}>[\d]{{2}})_'
                     '(?P<{3}>[\d]{{3}})_'
                     '(?P<{4}>[\d]{{3}})\.'
                     '(?P<{5}>drf)').format(anc_base_group,
                                            anc_product_group,
                                            anc_yy_start_group,
                                            anc_doy_start_group,
                                            anc_doy_end_group,
                                            file_pattern.general_extension_group)

ancillary_regex = re.compile(ancillary_pattern)

ancillary_eng_pattern = (r'^(?P<{0}>mvn)_'
                         '(?P<{1}>[a-z]{{3}})'
                         '(?P<{2}>[\d]{{2}})_'
                         '(?P<{3}>[\d]{{3}})_'
                         '(?P<{4}>[\d]{{3}})\.'
                         '(?P<{5}>dat|txt|drf)').format(anc_base_group,
                                                        anc_product_group,
                                                        anc_yy_start_group,
                                                        anc_doy_start_group,
                                                        anc_doy_end_group,
                                                        file_pattern.general_extension_group)

ancillary_eng_regex = re.compile(ancillary_eng_pattern)

anc_sci_svt_pattern = (r'^(?P<{0}>anc_sci)_'
                       '(?P<{1}>svt)'
                       '(?:|_(?P<{2}>[\d]{{4}})_'
                       '(?P<{3}>[\d]{{3}}))\.'
                       '(?P<{4}>drf)').format(anc_base_group,
                                              anc_product_group,
                                              anc_yyyy_start_group,
                                              anc_doy_start_group,
                                              file_pattern.general_extension_group)

anc_sci_svt_regex = re.compile(anc_sci_svt_pattern)

anc_app_pattern = (r'^(?P<{0}>mvn_app)_'
                   '(?P<{1}>[^_]+)_'
                   '(?P<{2}>[\d]{{6}})'
                   '(?:|_(?P<{3}>[\d]{{6}}))'
                   '(?:|_(?P<{4}>[^_]+))'
                   '_v(?P<{5}>[\d]{{2}})\.'
                   '(?P<{6}>bc)').format(anc_base_group,
                                         anc_product_group,
                                         file_pattern.general_yymmdd_group,
                                         file_pattern.general_yymmdd_group_end,
                                         anc_description_group,
                                         file_pattern.general_version_group,
                                         file_pattern.general_extension_group)

anc_app_regex = re.compile(anc_app_pattern)

anc_iuv_all_one_month_pattern = (r'^(?P<{0}>mvn)_'
                                 '(?P<{1}>iuv_all)_'
                                 'l(?P<{2}>[\d]*)_'
                                 '(?P<{3}>[\d]{{6}})_'
                                 '(?P<{4}>[\d]{{6}})'
                                 '_v(?P<{5}>[\d]{{2}})\.'
                                 '(?P<{6}>bc)').format(anc_base_group,
                                                       anc_product_group,
                                                       file_pattern.general_level_group,
                                                       file_pattern.general_yymmdd_group,
                                                       file_pattern.general_yymmdd_group_end,
                                                       file_pattern.general_version_group,
                                                       file_pattern.general_extension_group)

anc_iuv_all_one_month_regex = re.compile(anc_iuv_all_one_month_pattern)

anc_iuv_all_pattern = (r'^(?P<{0}>mvn)_'
                       '(?P<{1}>iuv_all)_'
                       'l(?P<{2}>[\d]*)_'
                       '(?P<{3}>[\d]{{8}})'
                       '(?:|_v(?P<{4}>[\d]{{3}}))\.'
                       '(?P<{5}>bc)').format(anc_base_group,
                                             anc_product_group,
                                             file_pattern.general_level_group,
                                             file_pattern.general_yyyymmdd_group,
                                             file_pattern.general_version_group,
                                             file_pattern.general_extension_group)

anc_iuv_all_regex = re.compile(anc_iuv_all_pattern)

anc_spice_general_pattern = (r'(?P<{0}>[^.]*)\.?'
                             '(?P<{1}>[0-9]+)?\.'  # optional product version
                             '(?P<{2}>tls|tsc|tpc|bsp|bc|ti|tf)').format(general_file_root_group,
                                                                         file_pattern.general_version_group,
                                                                         general_extension_group)

anc_spice_general_regex = re.compile(anc_spice_general_pattern)

rec_app_pattern = (r'^(?P<{0}>mvn)_'
                   '(?P<{1}>rec)_'
                   '(?P<{2}>[\d]{{6}})_'
                   '(?P<{3}>[\d]{{6}})'
                   '(?P<{4}>|_[^_]+)'
                   '_v(?P<{5}>[\d]+)\.'
                   '(?P<{6}>sff)').format(anc_base_group,
                                          anc_product_group,
                                          file_pattern.general_yymmdd_group,
                                          file_pattern.general_yymmdd_group_end,
                                          anc_description_group,
                                          file_pattern.general_version_group,
                                          file_pattern.general_extension_group)

rec_app_regex = re.compile(rec_app_pattern)

sfp_app_pattern = (r'^(?P<{0}>mvn)_'
                   '(?P<{1}>pred)_'
                   '(?P<{2}>[\d]{{6}})_'
                   '(?P<{3}>[\d]{{6}})_'
                   '(?P<{4}>|_[^_]+)'
                   '_v(?P<{5}>[\d]+)\.'
                   '(?P<{6}>sfp)').format(anc_base_group,
                                          anc_product_group,
                                          file_pattern.general_yymmdd_group,
                                          file_pattern.general_yymmdd_group_end,
                                          anc_description_group,
                                          file_pattern.general_version_group,
                                          file_pattern.general_extension_group)

sff_app_regex = re.compile(sfp_app_pattern)

sc_pattern = (r'(?P<{0}>mvn_sc)_'
              '(?P<{1}>[^_]+)_'
              '(?P<{2}>[\d]{{6}})_'
              '(?P<{3}>[\d]{{6}})'
              '(?P<{4}>|_[\w_]+)_'
              'v(?P<{5}>[\d]+)\.'
              '(?P<{6}>bc)').format(anc_base_group,
                                    anc_product_group,
                                    file_pattern.general_yymmdd_group,
                                    file_pattern.general_yymmdd_group_end,
                                    anc_description_group,
                                    file_pattern.general_version_group,
                                    file_pattern.general_extension_group)

sc_regex = re.compile(sc_pattern)

tst_pattern = (r'(?P<{0}>mvn)_'
               '(?P<{1}>tst)_'
               '(?P<{2}>[\d]{{6}})_'
               '(?P<{3}>[\d]{{6}})'
               '(?P<{4}>|_[\w_]+)_'
               'v(?P<{5}>[\d]+)\.'
               '(?P<{6}>sff)').format(anc_base_group,
                                      anc_product_group,
                                      file_pattern.general_yymmdd_group,
                                      file_pattern.general_yymmdd_group_end,
                                      anc_description_group,
                                      file_pattern.general_version_group,
                                      file_pattern.general_extension_group)

tst_regex = re.compile(tst_pattern)

optg_orb_pattern = (r'(?P<{0}>optg)_'
                    '(?P<{1}>orb)_'
                    '(?P<{2}>[\d]{{5}})-'
                    '(?P<{3}>[\d]{{5}})_'
                    '(?P<{4}>[a-z0-9_]+)_'
                    'v(?P<{5}>[\d]+)\.'
                    '(?P<{6}>txt)').format(anc_base_group,
                                           anc_product_group,
                                           anc_orb_start,
                                           anc_orb_end,
                                           anc_description_group,
                                           file_pattern.general_version_group,
                                           file_pattern.general_extension_group)
optg_orb_regex = re.compile(optg_orb_pattern)

optg_orb_pattern_2 = (r'(?P<{0}>optg)_'
                      '(?P<{1}>orb)_'
                      '(?P<{2}>[\d]{{6}})-'
                      '(?P<{3}>[\d]{{6}})_'
                      '(?P<{4}>[a-z0-9_]+)_'
                      'v(?P<{5}>[\d]+)\.'
                      '(?P<{6}>txt)').format(anc_base_group,
                                             anc_product_group,
                                             file_pattern.general_yymmdd_group,
                                             file_pattern.general_yymmdd_group_end,
                                             anc_description_group,
                                             file_pattern.general_version_group,
                                             file_pattern.general_extension_group)
optg_orb_regex_2 = re.compile(optg_orb_pattern_2)

anc_eps_pattern = (r'(?P<{0}>sci_anc)_'
                   '(?P<{1}>eps)'
                   '(?P<{2}>[\d]{{2}})_'
                   '(?P<{3}>[\d]{{3}})_'
                   '(?P<{4}>[\d]{{3}})_'
                   '(?P<{5}>[\w]+)\.'
                   '(?P<{6}>drf)').format(anc_base_group,
                                          anc_product_group,
                                          anc_yy_start_group,
                                          anc_doy_start_group,
                                          anc_doy_end_group,
                                          anc_description_group,
                                          file_pattern.general_extension_group)
anc_eps_regex = re.compile(anc_eps_pattern)

anc_spk_pattern = (r'(?P<{0}>spk)_'
                   '(?P<{1}>[\w]+)_'
                   '(?P<{2}>[\d]{{6}})-'
                   '(?P<{3}>[\d]{{6}})_'
                   '(?P<{4}>[^.]+)\.'
                   '(?P<{5}>bsp)'
                   '(?P<{6}>\.gz)*').format(anc_base_group,
                                            anc_product_group,
                                            file_pattern.general_yymmdd_group,
                                            file_pattern.general_yymmdd_group_end,
                                            anc_description_group,
                                            file_pattern.general_extension_group,
                                            anc_label_ext_group)
anc_spk_regex = re.compile(anc_spk_pattern)

anc_tsc_pattern = (r'(?P<{0}>MVN)_'
                   '(?P<{1}>SCLKSCET)\.'
                   '(?P<{2}>[\d]{{5}})\.'
                   '(?P<{3}>tsc)').format(anc_base_group,
                                          anc_product_group,
                                          file_pattern.general_version_group,
                                          file_pattern.general_extension_group)
anc_tsc_regex = re.compile(anc_tsc_pattern)

anc_trk_pattern = (r'(?P<{0}>[\d]{{2}})'
                   '(?P<{1}>[\d]{{3}})'
                   '(?:[\w]{{4}})'
                   '(?P<{2}>[\w]{{8}})'
                   '(?:[\w]{{2}})_'
                   '(?P<{3}>noHdr)\.'
                   '(?P<{4}>234)').format(anc_yy_start_group,
                                          anc_doy_start_group,
                                          anc_base_group,
                                          anc_product_group,
                                          file_pattern.general_extension_group)

anc_trk_regex = re.compile(anc_trk_pattern)

anc_trj_rec_pattern = (r'(?P<{0}>trj)_'
                       '(?P<{1}>orb)_'
                       '(?P<{2}>[\d]{{5}})-'
                       '(?P<{3}>[\d]{{5}})_'
                       '(?P<{4}>rec)_'
                       'v(?P<{5}>[\d]+)\.'
                       '(?P<{6}>bsp)').format(anc_base_group,
                                              anc_product_group,
                                              anc_orb_start,
                                              anc_orb_end,
                                              anc_description_group,
                                              file_pattern.general_version_group,
                                              file_pattern.general_extension_group)
anc_trj_rec_regex = re.compile(anc_trj_rec_pattern)

anc_trj_pred_pattern = (r'(?P<{0}>trj)_'
                        '(?P<{1}>orb)_'
                        '(?P<{2}>[\d]{{5}})-'
                        '(?P<{3}>[\d]{{5}})_'
                        '(?P<{4}>[\d]{{5}})'
                        '(?P<{5}>|_[^_]+)'
                        '_v(?P<{6}>[\d]+)\.'
                        '(?P<{7}>bsp)').format(anc_base_group,
                                               anc_product_group,
                                               anc_orb_start,
                                               anc_orb_end,
                                               anc_orb_predicted,
                                               anc_description_group,
                                               file_pattern.general_version_group,
                                               file_pattern.general_extension_group)
anc_trj_pred_regex = re.compile(anc_trj_pred_pattern)

anc_trj_oc_pattern = (r'(?P<{0}>trj_[o|c])_'
                      '(?P<{1}>|od[\w]{{4}}_)'
                      '(?P<{2}>[\d]{{6}})-'
                      '(?P<{3}>[\d]{{6}})'
                      '(?P<{4}>|_[^.]+)'
                      '_v(?P<{5}>[\d]+)\.'
                      '(?P<{6}>bsp)').format(anc_base_group,
                                             anc_product_group,
                                             file_pattern.general_yymmdd_group,
                                             file_pattern.general_yymmdd_group_end,
                                             anc_description_group,
                                             file_pattern.general_version_group,
                                             file_pattern.general_extension_group)

anc_trj_oc_regex = re.compile(anc_trj_oc_pattern)

anc_pred_sfp_pattern = (r'(?P<{0}>mvn_pred)_'
                        '(?P<{1}>[\d]{{6}})_'
                        '(?P<{2}>[\d]{{6}})_'
                        '(?P<{3}>.+)'
                        '(?P<{4}>v[\d]{{2}}|)\.'
                        '(?P<{5}>sfp)').format(anc_base_group,
                                               file_pattern.general_yymmdd_group,
                                               file_pattern.general_yymmdd_group_end,
                                               anc_description_group,
                                               file_pattern.general_version_group,
                                               file_pattern.general_extension_group)

anc_pred_sfp_regex = re.compile(anc_pred_sfp_pattern)

radio_resid_pattern = (r'^(?P<{0}>mvnma|202ma)'  # Spacecraft ID (202) and Target MA (Mars)
                         '(?P<{1}>.{{2}})'
                         '(?P<{2}>[\d]{{4}})'
                         '(?P<{3}>[\d]{{3}})_'
                         '(?P<{4}>[\d]{{2}})'
                         '(?P<{5}>[\d]{{2}})'
                         '(?P<{6}>[^\.]+)\.'
                         '(?P<{7}>[^\.]+)\.'
                         '(?P<{8}>resid)\.'
                         '(?P<{9}>txt)').format(anc_base_group,
                                                anc_product_group,
                                                anc_yyyy_start_group,
                                                anc_doy_start_group,
                                                file_pattern.general_hh_group,
                                                file_pattern.general_mm_group,
                                                anc_description_group,
                                                anc_rsr_subchannel,
                                                anc_rsr_id,
                                                file_pattern.general_extension_group)

radio_resid_regex = re.compile(radio_resid_pattern, re.IGNORECASE)

radio_data_pattern = (r'^(?P<{0}>mvnma|202ma)'
                         '(?P<{1}>.{{2}})'
                         '(?P<{2}>[\d]{{4}})(_|)'
                         '(?P<{3}>[\d]{{3}})_'
                         '(?P<{4}>[\d]{{2}})'
                         '(?P<{5}>[\d]{{2}})'
                         '(?P<{6}>[^\.]+)\.'
                         '(?P<{7}>tnf|[1-4][a-b][1-4]|dlf|fup|[a-c]0[1-4])').format(anc_base_group,
                                                                                    anc_product_group,
                                                                                    anc_yyyy_start_group,
                                                                                    anc_doy_start_group,
                                                                                    file_pattern.general_hh_group,
                                                                                    file_pattern.general_mm_group,
                                                                                    anc_description_group,
                                                                                    file_pattern.general_extension_group)

radio_data_regex = re.compile(radio_data_pattern, re.IGNORECASE)

radio_l3a_pattern = (r'^(?P<{0}>mvnma|202ma)'
                         '(?P<{1}>.{{2}})'
                         '(?P<{2}>[\d]{{4}})(_|)'
                         '(?P<{3}>[\d]{{3}})(_|)'
                         '(?P<{4}>[\d]{{4}})(_|)'
                         '(?P<{5}>[\d]{{3}})(_|)'
                         '(?P<{6}>|[\d]{{2}})\.'
                         '(?P<{7}>ion|tro|wea)').format(anc_base_group,
                                                        anc_product_group,
                                                        anc_yyyy_start_group,
                                                        anc_doy_start_group,
                                                        anc_yyyy_end_group,
                                                        anc_doy_end_group,
                                                        anc_description_group,
                                                        file_pattern.general_extension_group)

radio_l3a_regex = re.compile(radio_l3a_pattern, re.IGNORECASE)

ancillary_regex_list = [ancillary_regex,
                        ancillary_eng_regex,
                        anc_sci_svt_regex,
                        anc_app_regex,
                        anc_iuv_all_regex,
                        rec_app_regex,
                        sc_regex,
                        tst_regex,
                        optg_orb_regex,
                        optg_orb_regex_2,
                        anc_eps_regex,
                        anc_spk_regex,
                        anc_tsc_regex,
                        anc_trk_regex,
                        anc_trj_rec_regex,
                        anc_trj_pred_regex,
                        anc_trj_oc_regex,
                        anc_pred_sfp_regex,
                        radio_resid_regex,
                        radio_l3a_regex,
                        radio_data_regex]
