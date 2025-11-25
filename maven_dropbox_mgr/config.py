'''
This modules provides the Dropbox Manager rules for:
  - Detecting Dropbox files based on regular expressions applied to file names
  - Transforming files names
  - Determine dropbox locations

The PatternDestinationRule set 'file_rules' below defines these rules for the various
files types recognized by Dropbox manager.
'''
import logging
import re
import os
from collections import namedtuple

from maven_utilities import file_pattern, maven_config, anc_config, utilities
from maven_utilities.enums import DUPLICATE_ACTION
from functools import cmp_to_key

logger = logging.getLogger('maven.maven_dropbox_mgr.config.log')

date_level_format = '%Y%m%dT%H%M%S'

# Directory Definitions
root_destination_directory = '/maven/data/sci/'
root_ancillary_destination_directory = '/maven/data/anc/'
pocdrop_destination_directory = '/maven/poc/dropbox/'
invalid_dir_name = 'misnamed_files'
dupe_dir_name = 'duplicate_files'
metadata_dir_name = 'metadata'

PatternDestinationRule = namedtuple('PatternDestinationRule', ['pattern',
                                                               'groups_to_check',
                                                               'absolute_directories',
                                                               'duplicate_check',
                                                               'unzip_compressed'])

# Regular expression used to describe zip files that are to remain zipped when being moved by dropbox manager
zip_ignore_pattern = re.compile(r'$z')  # don't match anything for now.

# Maps file names to the collection level in the directory hierarchy
# NOTE - Order matters, precedence is from top to bottom
IuvsCollectionsMapping = namedtuple('IuvsCollectionsMapping', ['pattern', 'collection'])
SweaCollectionsMapping = namedtuple('SweaCollectionsMapping', ['pattern', 'collection'])
LpwDenCollectionsMapping = namedtuple('LpwDenCollectionsMapping', ['pattern', 'collection'])
iuvs_collections_mapping = [
    IuvsCollectionsMapping(re.compile('.*ech.*'), 'echelle'),
    IuvsCollectionsMapping(re.compile('.*phobos'), 'phobos'),
    IuvsCollectionsMapping(re.compile('.*centroid|.*sun.*|.*comm.*|.*star.*'), 'calibration'),
    IuvsCollectionsMapping(re.compile('.*outbound|.*inbound|.*outspace|.*inspace|.*corona'), 'corona'),
    IuvsCollectionsMapping(re.compile('.*ISON|.*IPH|.*cruisecal|.*checkout', re.I), 'cruise'),
    IuvsCollectionsMapping(re.compile('.*apoapse|.*outdisk|.*indisk|.*o3|.*reflectance'), 'disk'),
    IuvsCollectionsMapping(re.compile('.*periapse|.*outlimb|.*inlimb'), 'limb'),
    IuvsCollectionsMapping(re.compile('.*occultation'), 'occultation'),
    IuvsCollectionsMapping(re.compile('.*early|.*APP|.*comet', re.I), 'transition')
]
swea_collections_mapping = [
    SweaCollectionsMapping(re.compile('.*padscore.*'), 'padscore'),
    SweaCollectionsMapping(re.compile('.*shape.*'), 'shape'),
    SweaCollectionsMapping(re.compile('.*scpot.*'), 'scpot')
]
lpw_den_collections_mapping = [
    LpwDenCollectionsMapping(re.compile('.*den-orb-high-alt.*'), 'high-alt'),
    LpwDenCollectionsMapping(re.compile('.*den-orb-low-alt.*'), 'low-alt')
]


def iuvs_loc_generation(m):
    '''Method used to generate the absolute destination for a general dropbox file that meets the iuvs_science_pattern regular expression
    Returns:
        (root_dir,dynamic_dir)

    where root_dir is expected to reside on the system and dynamic_dir may or may not exist.
    '''

    assert m is not None
    collection_type = m.group(file_pattern.general_description_group)
    collection = None
    for next_mapping in iuvs_collections_mapping:
        m2 = next_mapping.pattern.match(collection_type)
        if m2 is None:
            continue
        collection = next_mapping.collection
        break

    if collection is None:
        logger.error('The IUVS collection type %s did not match any known patterns', collection_type)
        return None, None

    # /<root>/iuv/<level>/<collection>/<year>/<month>/
    return (os.path.join(root_destination_directory,
                         m.group(file_pattern.general_instrument_group),
                         m.group(file_pattern.general_level_group),
                         collection),
            os.path.join(m.group(file_pattern.general_year_group),
                         m.group(file_pattern.general_month_group))
            )


def sweal3_loc_generation(m):
    '''Method used to generate the absolute destination for a general dropbox file that meets the science_pattern regular expression
    Returns:
        (root_dir,dynamic_dir)

    where root_dir is expected to reside on the system and dynamic_dir may or may not exist.
    '''

    assert m is not None
    collection_type = m.group(file_pattern.general_description_group)
    collection = None
    for next_mapping in swea_collections_mapping:
        m2 = next_mapping.pattern.match(collection_type)
        if m2 is None:
            continue
        collection = next_mapping.collection
        break

    if collection is None:
        logger.error('The SWEA collection type %s did not match any known patterns', collection_type)
        return None, None

    # /<root>/swe/<level>/<collection>/<year>/<month>/
    return (os.path.join(root_destination_directory,
                         m.group(file_pattern.general_instrument_group),
                         m.group(file_pattern.general_level_group),
                         collection),
            os.path.join(m.group(file_pattern.general_year_group),
                         m.group(file_pattern.general_month_group))
            )


def lpwden_loc_generation(m):
    '''Method used to generate the absolute destination for a general dropbox file that meets the science_pattern regular expression
    Returns:
        (root_dir,dynamic_dir)

    where root_dir is expected to reside on the system and dynamic_dir may or may not exist.
    '''

    assert m is not None
    collection_type = m.group(file_pattern.general_description_group)
    collection = None
    for next_mapping in lpw_den_collections_mapping:
        m2 = next_mapping.pattern.match(collection_type)
        if m2 is None:
            continue
        collection = next_mapping.collection
        break

    if collection is None:
        logger.error('The LPW density collection type %s did not match any known patterns', collection_type)
        return None, None

    # /<root>/lpw/ql/<collection>/<year>/<month>/
    return (os.path.join(root_destination_directory,
                         m.group(file_pattern.general_instrument_group),
                         m.group(file_pattern.general_level_group),
                         collection),
            os.path.join(m.group(file_pattern.general_year_group),
                         m.group(file_pattern.general_month_group))
            )


def general_absolute_loc_generation(m):
    '''Method used to generate the absolute destination for a general dropbox file that meets the general_file_pattern regular expression
    Returns:
        (root_dir,dynamic_dir)

    where root_dir is expected to reside on the system and dynamic_dir may or may not exist.
    '''
    assert m is not None
    return (os.path.join(root_destination_directory,
                         m.group(file_pattern.general_instrument_group),
                         m.group(file_pattern.general_level_group)),
            os.path.join(m.group(file_pattern.general_year_group),
                         m.group(file_pattern.general_month_group))
            )


def playback_absolute_loc_generation(m):
    '''Method used to generate the absolute destination for a playback dropbox file that meets the playback_file_pattern regular expression
    Returns:
        (root_dir,dynamic_dir)

    where root_dir is expected to reside on the system and dynamic_dir may or may not exist.
    '''
    assert m is not None
    return (pocdrop_destination_directory,
            None)


def ql_absolute_loc_generation(m):
    '''Method used to generate the absolute destination for a quicklook dropbox file that meets the ql_file_pattern regular expression
    Returns:
        (root_dir,dynamic_dir)

    where root_dir is expected to reside on the system and dynamic_dir may or may not exist.
    '''
    assert m is not None
    orbit = m.group(maven_config.ql_orbit_group)

    return (os.path.join(root_destination_directory,
                         m.group(file_pattern.general_instrument_group),
                         m.group(file_pattern.general_level_group)),
            'single_orbit' if orbit is not None else None
            )


def euv_l2b_absolute_loc_generation(m):
    '''Method used to generate the absolute destination for a euv level 2b dropbox file that meets the euv_l2b_pattern regular expression
    Returns:
        (root_dir,dynamic_dir)

    where root_dir is expected to reside on the system and dynamic_dir does not exist.
    '''
    assert m is not None
    return (os.path.join(root_destination_directory,
                         m.group(file_pattern.general_instrument_group),
                         m.group(file_pattern.general_level_group)),
            None
            )

def euv_absolute_loc_generation(m):
    '''Method used to generate the absolute destination for an euv flare dropbox file that meets the euv_pattern, euv_flare_pattern, 
    and euv_flare_catalog_pattern regular expression.
    Returns:
        (root_dir, dynamic_dir)
        
        where root_dir is expected to reside on the system and dynamic_dir may or may not exist.
    '''
    assert m is not None
    return (os.path.join(root_destination_directory,
                         m.group(file_pattern.general_instrument_group),
                         "swx", 
                         "daily_plots", 
                        ),
            os.path.join(m.group(file_pattern.general_year_group),
                         m.group(file_pattern.general_month_group))
            )

def euv_flare_absolute_loc_generation(m):
    '''Method used to generate the absolute destination for an euv flare dropbox file that meets the euv_pattern, euv_flare_pattern, 
    and euv_flare_catalog_pattern regular expression.
    Returns:
        (root_dir, dynamic_dir)
        
        where root_dir is expected to reside on the system and dynamic_dir may or may not exist.
    '''
    assert m is not None
    return (os.path.join(root_destination_directory,
                         m.group(file_pattern.general_instrument_group),
                         "swx", 
                         "flare_plots"), 
            os.path.join(m.group(file_pattern.general_year_group),
                         m.group(file_pattern.general_month_group)),
            )

def euv_flare_catalog_absolute_loc_generation(m):
    '''Method used to generate the absolute destination for an euv flare dropbox file that meets the euv_pattern, euv_flare_pattern, 
    and euv_flare_catalog_pattern regular expression.
    Returns:
        (root_dir, dynamic_dir)
        
        where root_dir is expected to reside on the system and dynamic_dir may or may not exist.
    '''
    assert m is not None
    return (os.path.join(root_destination_directory,
                         m.group(file_pattern.general_instrument_group),
                         "swx", 
                         "flare_events"),
            None
            )

def sep_anc_absolute_loc_generation(m):
    '''Method used to generate the absolute destination for a sep ancillary dropbox file that meets the sep_anc_file_pattern regular expression
    Returns:
        (root_dir,dynamic_dir)

    where root_dir is expected to reside on the system and dynamic_dir may or may not exist.
    '''
    assert m is not None
    return (os.path.join(root_destination_directory,
                         m.group(file_pattern.general_instrument_group),
                         m.group(file_pattern.general_level_group)),
            os.path.join(m.group(file_pattern.general_year_group),
                         m.group(file_pattern.general_month_group))
            )


def kp_absolute_loc_generation(m):
    '''Method used to generate the absolute destination for an IUVS kp dropbox file that meets the kp_iuvs_pattern regular expression
    Returns:
        (root_dir,dynamic_dir)

    where root_dir is expected to reside on the system and dynamic_dir may or may not exist.
    '''
    assert m is not None
    return (os.path.join(root_destination_directory,
                         m.group(file_pattern.general_instrument_group),
                         m.group(file_pattern.general_level_group)),
            os.path.join(m.group(file_pattern.general_year_group),
                         m.group(file_pattern.general_month_group))
            )


def label_file_absolute_loc_generation(m):
    '''Method used to generate the absolute destination for a general dropbox file that meets the general_file_pattern regular expression
    Returns:
        (root_dir,dynamic_dir)

    where root_dir is expected to reside on the system and dynamic_dir may or may not exist.
    '''
    assert m is not None
    return (os.path.join(root_destination_directory,
                         m.group(file_pattern.general_instrument_group),
                         m.group(file_pattern.general_level_group)),
            os.path.join(m.group(file_pattern.general_year_group),
                         m.group(file_pattern.general_month_group))
            )


def metadata_absolute_loc_generation(m):
    '''Method used to generate the absolute destination for a metadata dropbox file
    Returns:
        (root_dir,dynamic_dir)

    where root_dir is expected to reside on the system and dynamic_dir may or may not exist.
    '''
    assert m is not None
    return (os.path.join(root_destination_directory,
                         m.group(file_pattern.general_instrument_group)[:3]),
            metadata_dir_name)


def sis_absolute_loc_generation(m):
    '''Method used to generate the absolute destination for a sis dropbox file
    Returns:
        (root_dir,dynamic_dir)

    where root_dir is expected to reside on the system and dynamic_dir may or may not exist.
    '''
    assert m is not None
    return (os.path.join(root_destination_directory,
                         m.group(file_pattern.general_instrument_group)[:3]),
            metadata_dir_name)


def caveats_absolute_loc_generation(m):
    '''Method used to generate the absolute destination for a caveats dropbox file
    Returns:
        (root_dir,dynamic_dir)

    where root_dir is expected to reside on the system and dynamic_dir may or may not exist.
    '''
    assert m is not None
    return (os.path.join(root_destination_directory,
                         m.group(file_pattern.general_instrument_group)[:3]),
            metadata_dir_name)


def radio_absolute_loc_generation(m):
    '''Method used to generate the absolute destination for a JPL Radio dropbox files
    Returns:
        (root_dir,dynamic_dir)

    where root_dir is expected to reside on the system and dynamic_dir may or may not exist.
    '''
    assert m is not None
    
    radio_extension = m.group(file_pattern.general_extension_group).lower()

    if radio_extension in ('dlf', 'fup', 'txt', 'wea'):
        radio_dir_name = 'rse/l2a/'
    elif radio_extension in ('ion', 'tro'):
        radio_dir_name = 'rse/l3a/'
    else:  # Must be a TNF or an RSR file
        radio_dir_name = 'rse/l0a/'
    
    return (os.path.join(root_ancillary_destination_directory),
            radio_dir_name)


def file_duplicate_check(src_fn, dst_fn):
    '''Method used to determine if source and destination files match and suggest an action
    Arguments:
        src_fn - the full name and path of the file you want to check for duplicates of 
        dest_fn - the full name and path where src_fn will be moved
    '''
    if not os.path.isfile(src_fn) or not os.path.isfile(dst_fn):
        return DUPLICATE_ACTION.IGNORE, None  # Not the same file
    if os.path.basename(src_fn) == os.path.basename(dst_fn):
        if utilities.files_are_same(src_fn, dst_fn):
            return DUPLICATE_ACTION.REMOVE, None  # Name and contents match
        return DUPLICATE_ACTION.OVERWRITE_ARCHIVE, dst_fn  # Contents have changed
    return DUPLICATE_ACTION.IGNORE, None  # Not the same file


file_rules = [
    PatternDestinationRule(anc_config.radio_resid_regex,
                           None,
                           radio_absolute_loc_generation,
                           file_duplicate_check,
                           False),
    PatternDestinationRule(anc_config.radio_l3a_regex,
                           None,
                           radio_absolute_loc_generation,
                           file_duplicate_check,
                           False),
    PatternDestinationRule(anc_config.radio_data_regex,
                           None,
                           radio_absolute_loc_generation,
                           None,
                           True),
    PatternDestinationRule(maven_config.metadata_regex,
                           None,
                           metadata_absolute_loc_generation,
                           None,
                           True),
    PatternDestinationRule(maven_config.sis_regex,
                           None,
                           sis_absolute_loc_generation,
                           None,
                           True),
    PatternDestinationRule(maven_config.metadata_caveats_regex,
                           None,
                           caveats_absolute_loc_generation,
                           None,
                           True),
    PatternDestinationRule(maven_config.metadata_readme_regex,
                           None,
                           metadata_absolute_loc_generation,
                           None,
                           True),
    PatternDestinationRule(maven_config.metadata_version_changes_regex,
                           None,
                           metadata_absolute_loc_generation,
                           None,
                           True),
    PatternDestinationRule(maven_config.science_regex,
                           [(file_pattern.general_instrument_group, re.compile('lpw')),
                            (file_pattern.general_level_group, re.compile(r'ql'))],
                           lpwden_loc_generation,
                           None,
                           False),
    PatternDestinationRule(maven_config.science_regex,
                           [(file_pattern.general_instrument_group, re.compile('swe')),
                            (file_pattern.general_level_group, re.compile(r'l3'))],
                           sweal3_loc_generation,
                           None,
                           False),
    PatternDestinationRule(maven_config.science_regex,
                           [(file_pattern.general_instrument_group, maven_config.iuv_instrument_group_regex)],
                           iuvs_loc_generation,
                           None,
                           False),
    PatternDestinationRule(maven_config.label_regex,
                           [(file_pattern.general_instrument_group, maven_config.iuv_instrument_group_regex)],
                           iuvs_loc_generation,
                           None,
                           False),
    PatternDestinationRule(maven_config.ql_regex,
                           None,
                           ql_absolute_loc_generation,
                           None,
                           True),
    PatternDestinationRule(maven_config.playback_file_regex,
                           None,
                           playback_absolute_loc_generation,
                           None,
                           True),
    PatternDestinationRule(maven_config.kp_regex,
                           None,
                           kp_absolute_loc_generation,
                           None,
                           True),
    PatternDestinationRule(maven_config.sep_anc_regex,
                           None,
                           sep_anc_absolute_loc_generation,
                           None,
                           True),
    PatternDestinationRule(maven_config.euv_l2b_regex,
                           None,
                           euv_l2b_absolute_loc_generation,
                           None,
                           True),
    PatternDestinationRule(maven_config.label_regex,
                           None,
                           label_file_absolute_loc_generation,
                           None,
                           True),
    PatternDestinationRule(maven_config.science_regex,
                           None,
                           general_absolute_loc_generation,
                           None,
                           True),  # catch all for general science files
    PatternDestinationRule(maven_config.euv_regex,
                           None,
                           euv_absolute_loc_generation,
                           None,
                           True),
    PatternDestinationRule(maven_config.euv_flare_regex,
                           None,
                           euv_flare_absolute_loc_generation,
                           None,
                           True),
    PatternDestinationRule(maven_config.euv_flare_catalog_regex,
                           None,
                           euv_flare_catalog_absolute_loc_generation,
                           None,
                           True),
    PatternDestinationRule(maven_config.euv_l4_regex,
                           None,
                           general_absolute_loc_generation,
                           None,
                           True),
]

# file mod time must be at least this old
# before we consider moving it
age_limit = 60.0  # one minute ago
