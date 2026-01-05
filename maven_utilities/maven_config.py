'''
Created on Sep 10, 2015

@author: cosc3564
'''
import re
from maven_utilities import file_pattern

'''
Contains the MAVEN mission-specific regexes from file_pattern
'''
# Ancillary group definitions
anc_base_group = 'anc_base'
anc_product_group = 'anc_product'
anc_year_group = 'anc_year'
anc_doy_start_group = 'anc_start_doy'
anc_doy_end_group = 'anc_end_doy'
anc_year_group_end = 'anc_year_start'
anc_year_group_end = 'anc_year_end'

# Group definitions
iuvs_level_group_regex = re.compile('iuvs')
insitu_level_group_regex = re.compile('insitu')
iuv_instrument_group_regex = re.compile('iuv')

# Orbit group definitions
orbit_orbit_group = 'orbit'

# Orbit pattern
orbit_pattern = (r'orbit(?P<{0}>[0-9]+)$').format(orbit_orbit_group)
orbit_regex = re.compile(orbit_pattern)

# ql group definitions
ql_orbit_group = 'ql_orbit'
euv_orbit_group = 'euv_orbit'

# quicklook pattern
ql_pattern = (r'^mvn_(?P<{0}>[a-z0-9]+)_'
              '(?P<{1}>[a-z0-9]+)'
              '(?P<{2}>|_[a-zA-Z0-9\-]+)_'
              '(?P<{3}>[0-9]{{4}})'
              '(?P<{4}>[0-9]{{2}})'
              '(?P<{5}>[0-9]{{2}})'
              '(?:|_(?P<{6}>[0-9]+))\.'
              '(?P<{7}>png|csv)').format(file_pattern.general_instrument_group,
                                         file_pattern.general_level_group,
                                         file_pattern.general_description_group,
                                         file_pattern.general_year_group,
                                         file_pattern.general_month_group,
                                         file_pattern.general_day_group,
                                         ql_orbit_group,
                                         file_pattern.general_extension_group)

ql_regex = re.compile(ql_pattern)

# l0 group definitions
l0_base_group = 'l0_base'
l0_grouping_group = 'l0_grouping'

# l0 pattern
l0_pattern = (r'(?P<{0}>'
              '(?P<{1}>'
              'mvn_(?P<{2}>[a-zA-Z0-9]*)_'
              '(?P<{3}>[a-zA-Z0-9]*)_'
              '(?P<{4}>l0[a-z]*)_'
              '(?P<{5}>[\d]{{4}})'
              '(?P<{6}>[\d]{{2}})'
              '(?P<{7}>[\d]{{2}})'
              ')'  # end l0_base_group
              '(?:|_v(?P<{8}>[\d]*))'
              ')\.'  # end general_basename_group
              '(?P<{9}>dat)$').format(file_pattern.general_basename_group,
                                      l0_base_group,
                                      file_pattern.general_instrument_group,
                                      l0_grouping_group,
                                      file_pattern.general_level_group,
                                      file_pattern.general_year_group,
                                      file_pattern.general_month_group,
                                      file_pattern.general_day_group,
                                      file_pattern.general_version_group,
                                      file_pattern.general_extension_group)

l0_regex = re.compile(l0_pattern)

# playback group definitions
pfp_playback_data_group = 'playback_data'

# playback pattern
playback_file_pattern = (r'(?P<{0}>pfp)_'
                         '(?P<{1}>[a-z0-9]+)_'
                         'playback\.'
                         '(?P<{2}>[a-z0-9]+)\.'
                         '(?P<{3}>[0-9]+)').format(file_pattern.general_instrument_group,
                                                   pfp_playback_data_group,
                                                   file_pattern.general_description_group,
                                                   file_pattern.general_extension_group)

playback_file_regex = re.compile(playback_file_pattern)

# metadata group definitions
meta_type_group = 'meta_type'
meta_description = 'meta_description'

metadata_pattern = (r'^(?P<{0}>collection|bundle)_'
                    '(?P<{1}>maven_|)'
                    '(?P<{2}>[a-zA-Z0-9]+)'
                    '(?P<{3}>_[a-zA-Z0-9]+|)'
                    '(?P<{4}>_[0-9a-zA-Z_]+|)\.'
                    '(?P<{5}>xml|tab|csv)'
                    '(?P<{6}>\.gz)*').format(meta_type_group,
                                             file_pattern.general_mission_group,
                                             file_pattern.general_instrument_group,
                                             file_pattern.general_level_group,
                                             meta_description,
                                             file_pattern.general_extension_group,
                                             file_pattern.general_gz_extension_group)
metadata_regex = re.compile(metadata_pattern)

metadata_readme_pattern = (r'(?P<{0}>[0-9a-zA-Z_]+)_'
                           '(?P<{1}>readme)\.'
                           '(?P<{2}>txt|xml)'
                           '(?P<{3}>\.gz)*').format(file_pattern.general_instrument_group,
                                                    meta_type_group,
                                                    file_pattern.general_extension_group,
                                                    file_pattern.general_gz_extension_group)

metadata_readme_regex = re.compile(metadata_readme_pattern)

metadata_version_changes_pattern = (r'(?P<{0}>[a-zA-Z0-9]+)_'
                                    '(?P<{1}>[a-zA-Z0-9]+)_'
                                    '(?P<{2}>version_changes)'
                                    '(?P<{3}>|_v(?P<{4}>[0-9]+)_r(?P<{5}>[0-9]+))\.'
                                    '(?P<{6}>pdf|xml)'
                                    '(?P<{7}>\.gz)*').format(file_pattern.general_instrument_group,
                                                             file_pattern.general_level_group,
                                                             meta_type_group,
                                                             file_pattern.general_version_revision_group,
                                                             file_pattern.general_version_group,
                                                             file_pattern.general_revision_group,
                                                             file_pattern.general_extension_group,
                                                             file_pattern.general_gz_extension_group)

metadata_version_changes_regex = re.compile(metadata_version_changes_pattern)

# general science pattern
science_pattern = (r'^mvn_(?P<{0}>[a-zA-Z0-9]+)_'
                   '(?P<{1}>ql|l[a-zA-Z0-9]+)'
                   '(?P<{2}>|_[a-zA-Z0-9\-]+)_'
                   '(?P<{3}>[0-9]{{4}})'
                   '(?P<{4}>[0-9]{{2}})'
                   '(?P<{5}>[0-9]{{2}})'
                   '(?P<{6}>|T[0-9]{{6}}|t[0-9]{{6}})_'
                   'v(?P<{7}>[0-9]+)_'
                   'r(?P<{8}>[0-9]+)\.'
                   '(?P<{9}>jpg|png|fits|cdf|txt|csv|md5|tab|sts|sav|pdf|dat)'
                   '(?P<{10}>\.gz)*').format(file_pattern.general_instrument_group,
                                             file_pattern.general_level_group,
                                             file_pattern.general_description_group,
                                             file_pattern.general_year_group,
                                             file_pattern.general_month_group,
                                             file_pattern.general_day_group,
                                             file_pattern.general_hhmmss_group,
                                             file_pattern.general_version_group,
                                             file_pattern.general_revision_group,
                                             file_pattern.general_extension_group,
                                             file_pattern.general_gz_extension_group)

science_regex = re.compile(science_pattern)

# label pattern
label_pattern = (r'^mvn_(?P<{0}>[a-zA-Z0-9]+)_'
                 '(?P<{1}>iuvs|insitu|ql|l[a-zA-Z0-9]+)'
                 '(?P<{2}>|_[a-zA-Z0-9\-]+)_'
                 '(?P<{3}>[0-9]{{4}})'
                 '(?P<{4}>[0-9]{{2}})'
                 '(?P<{5}>[0-9]{{2}})'
                 '(?P<{6}>|t[0-9]{{6}}|T[0-9]{{6}})'
                 '(?P<{7}>|_v(?P<{8}>[0-9]{{2}})_r(?P<{9}>[0-9]{{2}}))\.'
                 '(?P<{10}>xml)'
                 '(?P<{11}>\.gz)*').format(file_pattern.general_instrument_group,
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
                                           file_pattern.general_gz_extension_group
                                           )

label_regex = re.compile(label_pattern)

# kp pattern
kp_pattern = (r'^mvn_(?P<{0}>kp)_'
              '(?P<{1}>insitu|iuvs)'
              '(?P<{2}>|_[a-zA-Z0-9\-]+)_'
              '(?P<{3}>[0-9]{{4}})'
              '(?P<{4}>[0-9]{{2}})'
              '(?P<{5}>[0-9]{{2}})'
              '(?P<{6}>|[t|T][0-9]{{6}})_'
              'v(?P<{7}>[0-9]+)_r(?P<{8}>[0-9]+)\.'
              '(?P<{9}>tab)'
              '(?P<{10}>\.gz)*').format(file_pattern.general_instrument_group,
                                        file_pattern.general_level_group,
                                        file_pattern.general_description_group,
                                        file_pattern.general_year_group,
                                        file_pattern.general_month_group,
                                        file_pattern.general_day_group,
                                        file_pattern.general_hhmmss_group,
                                        file_pattern.general_version_group,
                                        file_pattern.general_revision_group,
                                        file_pattern.general_extension_group,
                                        file_pattern.general_gz_extension_group)

kp_regex = re.compile(kp_pattern)

# SEP ancillary pattern
# TODO - See if this can be merged with the general ancillary
sep_anc_pattern = (r'^mvn_(?P<{0}>sep)_'
                   '(?:l2_|)'
                   '(?P<{1}>anc|pad)'
                   '(?P<{2}>|_[a-zA-Z0-9\-]+)_'
                   '(?P<{3}>[0-9]{{4}})'
                   '(?P<{4}>[0-9]{{2}})'
                   '(?P<{5}>[0-9]{{2}})'
                   '(?P<{6}>|[t|T][0-9]{{6}})_'
                   'v(?P<{7}>[0-9]+)_r(?P<{8}>[0-9]+)\.'
                   '(?P<{9}>sav|cdf)').format(file_pattern.general_instrument_group,
                                              file_pattern.general_level_group,
                                              file_pattern.general_description_group,
                                              file_pattern.general_year_group,
                                              file_pattern.general_month_group,
                                              file_pattern.general_day_group,
                                              file_pattern.general_hhmmss_group,
                                              file_pattern.general_version_group,
                                              file_pattern.general_revision_group,
                                              file_pattern.general_extension_group)

sep_anc_regex = re.compile(sep_anc_pattern)

euv_l2b_pattern = (r'^mvn_(?P<{0}>euv)_'
                   '(?P<{1}>l2b|l3b)_'
                   '(?P<{2}>orbit_merged)_'
                   '(?P<{3}>|[0-9]{{4}})'
                   '(?P<{4}>|[0-9]{{2}})'
                   '(?P<{5}>|[0-9]{{2}})'
                   '(?P<{6}>|[t|T][0-9]{{6}}_)'
                   'v(?P<{7}>[0-9]+)_'
                   'r(?P<{8}>[0-9]+)\.'
                   '(?P<{9}>sav|nc)'
                   '(?P<{10}>\.gz)*').format(file_pattern.general_instrument_group,
                                             file_pattern.general_level_group,
                                             file_pattern.general_description_group,
                                             file_pattern.general_year_group,
                                             file_pattern.general_month_group,
                                             file_pattern.general_day_group,
                                             file_pattern.general_hhmmss_group,
                                             file_pattern.general_version_group,
                                             file_pattern.general_revision_group,
                                             file_pattern.general_extension_group,
                                             file_pattern.general_gz_extension_group)

euv_l2b_regex = re.compile(euv_l2b_pattern)

euv_pattern = (r'^mvn_(?P<{0}>euv)_'
                   '(?P<{1}>|[0-9]{{4}})'
                   '(?P<{2}>|[0-9]{{2}})'
                   '(?P<{3}>|[0-9]{{2}})_'
                   '(?P<{4}>|[0-9]{{4}})\.'
                   '(?P<{5}>png)').format(file_pattern.general_instrument_group,
                                          file_pattern.general_year_group,
                                          file_pattern.general_month_group,
                                          file_pattern.general_day_group,
                                          file_pattern.general_hhmmss_group,
                                          file_pattern.general_extension_group)

euv_regex = re.compile(euv_pattern)

euv_flare_pattern = (r'^mvn_(?P<{0}>euv)_flare_'
                   '(?P<{1}>|[0-9]{{4}})'
                   '(?P<{2}>|[0-9]{{2}})'
                   '(?P<{3}>|[0-9]{{2}})_'
                   '(?P<{4}>|[0-9]{{4}})_'
                   '(?P<{5}>|\S{{4}})\.'
                   '(?P<{6}>png)').format(file_pattern.general_instrument_group,
                                          file_pattern.general_year_group,
                                          file_pattern.general_month_group,
                                          file_pattern.general_day_group,
                                          file_pattern.general_hhmmss_group,
                                          file_pattern.general_flare_class,
                                          file_pattern.general_extension_group)

euv_flare_regex = re.compile(euv_flare_pattern)

euv_flare_catalog_pattern = (r'^mvn_(?P<{0}>euv)_flare_catalog_'
                   '(?P<{1}>|[0-9]{{4}})\.'
                   '(?P<{2}>txt)').format(file_pattern.general_instrument_group,
                                          file_pattern.general_year_group,
                                          file_pattern.general_extension_group)

euv_flare_catalog_regex = re.compile(euv_flare_catalog_pattern)


# general science pattern
euv_l4_pattern = (r'^mvn_(?P<{0}>[a-zA-Z0-9]+)_'
                   '(?P<{1}>ql|l[a-zA-Z0-9]+)'
                   '(?P<{2}>|_[a-zA-Z0-9\-]+)'
                   '(?P<{3}>|_[a-zA-Z0-9\-]+)_'
                   '(?P<{4}>[0-9]{{4}})'
                   '(?P<{5}>[0-9]{{2}})'
                   '(?P<{6}>[0-9]{{2}})'
                   '(?P<{7}>|T[0-9]{{6}}|t[0-9]{{6}})_'
                   'v(?P<{8}>[0-9]+)_'
                   'r(?P<{9}>[0-9]+)\.'
                   '(?P<{10}>csv|xml)'
                   '(?P<{11}>\.gz)*').format(file_pattern.general_instrument_group,
                                             file_pattern.general_level_group,
                                             file_pattern.general_description_group,
                                             euv_orbit_group,
                                             file_pattern.general_year_group,
                                             file_pattern.general_month_group,
                                             file_pattern.general_day_group,
                                             file_pattern.general_hhmmss_group,
                                             file_pattern.general_version_group,
                                             file_pattern.general_revision_group,
                                             file_pattern.general_extension_group,
                                             file_pattern.general_gz_extension_group)

euv_l4_regex = re.compile(euv_l4_pattern)


sis_pattern = (r'^(?P<{0}>[a-zA-Z0-9]+)_'
               '(?P<{1}>[0-9a-zA-Z]+)_'
               '(?P<{2}>sis)'
               '(?:|_v(?P<{3}>[\d]+)_r(?P<{4}>[\d]+))\.'
               '(?P<{5}>pdf|xml)'
               '(?P<{6}>\.gz)*').format(file_pattern.general_instrument_group,
                                        file_pattern.general_level_group,
                                        meta_type_group,
                                        file_pattern.general_version_group,
                                        file_pattern.general_revision_group,
                                        file_pattern.general_extension_group,
                                        file_pattern.general_gz_extension_group)

sis_regex = re.compile(sis_pattern)

# General pattern to detect and process version/revision file names
root_verrev_ext_pattern = (r'(?P<{0}>[a-zA-Z0-9_\-]+?)'  # ? lazy consumption to honor the optional _v below
                           '(?:|_v(?P<{1}>[0-9]+))'
                           '(?:|_r(?P<{2}>[0-9]+))\.'
                           '(?P<{3}>.*)').format(file_pattern.general_root_group,
                                                 file_pattern.general_version_group,
                                                 file_pattern.general_revision_group,
                                                 file_pattern.general_extension_group)

root_verrev_ext_regex = re.compile(root_verrev_ext_pattern)

metadata_caveats_pattern = (r'(?P<{0}>[a-zA-Z0-9]+)_'
                            '(?P<{1}>[a-zA-Z0-9]+)_'
                            '(?P<{2}>caveats)'
                            '(?P<{3}>|_v(?P<{4}>[0-9]+)_r(?P<{5}>[0-9]+))\.'
                            '(?P<{6}>xlsx|pdf|xml)'
                            '(?P<{7}>\.gz)*').format(file_pattern.general_instrument_group,
                                                     file_pattern.general_level_group,
                                                     meta_type_group,
                                                     file_pattern.general_version_revision_group,
                                                     file_pattern.general_version_group,
                                                     file_pattern.general_revision_group,
                                                     file_pattern.general_extension_group,
                                                     file_pattern.general_gz_extension_group)

metadata_caveats_regex = re.compile(metadata_caveats_pattern)

tracking_file_pattern = (r'^(?P<{0}>[0-9]{{2}})'
                         '(?P<{1}>[0-9]{{3}})'
                         '[a-zA-Z0-9_]+\.'
                         '(?P<{2}>234)').format(file_pattern.general_year_group,
                                                file_pattern.general_doy_group,
                                                file_pattern.general_extension_group)

tracking_file_regex = re.compile(tracking_file_pattern)

tracking_bundle_pattern = (r'^mvn_trk_anc_'
                           '^(?P<{0}>[0-9]{{2}})'
                           '(?P<{1}>[0-9]{{3}})_'
                           '^(?P<{2}>[0-9]{{2}})'
                           '(?P<{3}>[0-9]{{3}}).'
                           '(?P<{4}>tgz)').format(anc_year_group,
                                                  anc_doy_start_group,
                                                  anc_year_group_end,
                                                  anc_doy_end_group,
                                                  file_pattern.general_extension_group)

tracking_bundle_regex = re.compile(tracking_bundle_pattern)
