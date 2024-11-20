'''
Created on Aug 26, 2015

This modules provides the Dropbox Manager rules for:
  - Detecting Dropbox files based on regular expressions applied to file names
  - Transforming files names
  - Determine dropbox locations

The PatternDestinationRule set 'file_rules' below defines these rules for the various
files types recognized by Dropbox manager.

@author: Bryan Staley
'''
import logging
import re
import os
import csv
from collections import namedtuple

from maven_utilities import file_pattern, maven_config, constants, anc_config, utilities
from maven_utilities.enums import DUPLICATE_ACTION
from maven_dropbox_mgr import init_time
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
                                                               'filename_transform',
                                                               'absolute_directories',
                                                               'transform_record_keeping',
                                                               'duplicate_check',
                                                               'unzip_compressed'])

# Regular expression used to describe zip files that are to remain zipped when being moved by dropbox manager
zip_ignore_pattern = re.compile(r'$z')  # don't match anything for now.

# Maps file names to the collection level in the directory hierarchy
# NOTE - Order matters, precedence is from top to bottom
IuvsCollectionsMapping = namedtuple('IuvsCollectionsMapping', ['pattern', 'collection'])
SweaCollectionsMapping = namedtuple('SweaCollectionsMapping', ['pattern', 'collection'])
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


def metadata_filename_transform(m):
    '''Method used to transform the provided name to maven_config.metadata_index_pattern
    Arguments:
      m - the regex group match
    Returns:
      The transformed file name.
    '''
    assert m is not None

    date_level = init_time.strftime(date_level_format)
    level = m.group(file_pattern.general_level_group).replace('_', '')
    instrument = m.group(file_pattern.general_instrument_group)[:3]
    description = m.group(maven_config.meta_description)[1:]  # strip leading _
    # Converts _ to - if _ are found in the description
    description = description.replace('_', '-')

    level = level or 'nolevel'  # default level if none given

    ret_val = 'mvn_{}_{}_{}'.format(instrument,
                                    level,
                                    m.group(maven_config.meta_type_group))

    ret_val += '_{}'.format(description) if description is not None and len(description) > 1 else ''

    # Adds _YYYYMMDDTHHMMSS to the files as well as the extension
    ret_val += '_{}.{}'.format(date_level,
                               m.group(file_pattern.general_extension_group))

    ret_val += m.group(file_pattern.general_gz_extension_group) if m.group(file_pattern.general_gz_extension_group) is not None else ''

    return ret_val


def sis_filename_transform(m):
    '''Method used to transform the provided name to maven_config.metadata_index_pattern
    Arguments:
      m - the regex group match
    Returns:
      The transformed file name.
    '''
    assert m is not None

    date_level = init_time.strftime(date_level_format)
    level = m.group(file_pattern.general_level_group).replace('_', '')
    instrument = m.group(file_pattern.general_instrument_group)[:3]
    version = m.group(file_pattern.general_version_group)
    revision = m.group(file_pattern.general_revision_group)

    ret_val = 'mvn_{}_{}_{}'.format(instrument,
                                    level,
                                    m.group(maven_config.meta_type_group))

    ret_val += '_{}'.format(date_level)

    if version and revision:
        ret_val += '_v{}_r{}'.format(version, revision)

    ret_val += '.{}'.format(m.group(file_pattern.general_extension_group))

    ret_val += m.group(file_pattern.general_gz_extension_group) if m.group(file_pattern.general_gz_extension_group) is not None else ''

    return ret_val


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


def caveats_filename_transform(m):
    '''Method used to transform the provided name to maven_config.metadata_index_pattern
    Arguments:
      m - the regex group match
    Returns:
      The transformed file name.
    '''
    assert m is not None

    date_level = init_time.strftime(date_level_format)
    level = m.group(file_pattern.general_level_group).replace('_', '')
    instrument = m.group(file_pattern.general_instrument_group)[:3]

    ret_val = 'mvn_{}_{}_{}_{}'.format(instrument,
                                       level,
                                       m.group(maven_config.meta_type_group),
                                       date_level)
    # Add version revision
    ret_val += '{}'.format(m.group(file_pattern.general_version_revision_group)) if m.group(file_pattern.general_version_revision_group) is not None else ''

    ret_val += '.{}'.format(m.group(file_pattern.general_extension_group))

    ret_val += m.group(file_pattern.general_gz_extension_group) if m.group(file_pattern.general_gz_extension_group) is not None else ''

    return ret_val


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
    

def metadata_duplicate_check(src_fn, dest_fn):
    '''Method used to check if there is an older file name with a matching 
       maven_config.metadata_index_pattern to dest_fn.  If there is, and their checksums
       match, return True
    Arguments:
      src_fn - the full name and path of the file you want to check for duplicates of 
      dest_fn - the full name and path where src_fn will be moved
    Returns:
      True if there is an identical file, else False
    '''
    dest_directory, filename = os.path.split(dest_fn)
    matching_basename = get_matching_metadata_filename(dest_directory, filename)
    if matching_basename is not None:
        matching_fn = os.path.join(dest_directory, matching_basename)
        if utilities.files_are_same(src_fn, matching_fn):
            logger.info("%s and %s are the same file.", src_fn, matching_fn)
            return DUPLICATE_ACTION.REMOVE, None
        return DUPLICATE_ACTION.OVERWRITE_ARCHIVE , matching_fn
    return DUPLICATE_ACTION.IGNORE, None


def get_matching_metadata_filename(dest_directory, basename):
    '''Returns a filename in dest_directory that matches basename and has the largest date.
       If no matches are found, returns None
    '''
    dest_filenames = utilities.listdir_files(dest_directory)
    matching_filenames = [fn for fn in dest_filenames if matches_metadata_filename(basename, fn)]
    if len(matching_filenames) > 0:
        sorted_matching_filenames = sorted(matching_filenames, key=cmp_to_key(cmp_metadata_versions))
        logger.debug("sorted matched src filename: %s", sorted_matching_filenames[-1])
        return sorted_matching_filenames[-1]
    logger.debug("%s does NOT match a filename in destination directory %s", basename, dest_directory)
    return None


def matches_metadata_filename(src_filename, dest_filename):
    '''Returns True if the dest_filename matches the src_filename in all areas except the date
    '''
    src_m = file_pattern.extract_parts([maven_config.metadata_index_regex],
                                       src_filename,
                                       [file_pattern.general_instrument_group,
                                        file_pattern.general_level_group,
                                        maven_config.meta_type_group,
                                        maven_config.meta_description,
                                        file_pattern.general_version_revision_group,
                                        file_pattern.general_version_group,
                                        file_pattern.general_revision_group,
                                        file_pattern.general_extension_group,
                                        file_pattern.general_gz_extension_group])
    assert src_m is not None
    dest_m = file_pattern.extract_parts([maven_config.metadata_index_regex],
                                        dest_filename,
                                        [file_pattern.general_instrument_group,
                                         file_pattern.general_level_group,
                                         maven_config.meta_type_group,
                                         maven_config.meta_description,
                                         file_pattern.general_version_revision_group,
                                         file_pattern.general_version_group,
                                         file_pattern.general_revision_group,
                                         file_pattern.general_extension_group,
                                         file_pattern.general_gz_extension_group])
    return src_m == dest_m


def cmp_metadata_versions(fn1, fn2):
    '''Compares two filenames based on their versions.'''

    def _cmp(a, b):
        return (a > b) - (a < b)

    return _cmp(get_metadata_version(fn1), get_metadata_version(fn2))


def get_metadata_version(filename):
    '''Returns the date that is in the filename.'''
    m = file_pattern.extract_parts([maven_config.metadata_index_regex],
                                   filename,
                                   parts=[file_pattern.general_year_group,
                                          file_pattern.general_month_group,
                                          file_pattern.general_day_group,
                                          file_pattern.general_hhmmss_group])
    assert m is not None
    ver = ''.join(m.values())
    return ver


def version_changes_filename_transform(m):
    '''Method used to transform the provided name to maven_config.metadata_index_pattern
    Arguments:
      m - the regex group match
    Returns:
      The transformed file name.
    '''
    assert m is not None

    date_level = init_time.strftime(date_level_format)
    level = m.group(file_pattern.general_level_group).replace('_', '')
    meta_type = m.group(maven_config.meta_type_group).replace('_', '-')
    instrument = m.group(file_pattern.general_instrument_group)[:3]

    ret_val = 'mvn_{}_{}_{}_{}'.format(instrument,
                                       level,
                                       meta_type,
                                       date_level)
    # Add version revision
    ret_val += '{}'.format(m.group(file_pattern.general_version_revision_group)) if m.group(file_pattern.general_version_revision_group) is not None else ''

    ret_val += '.{}'.format(m.group(file_pattern.general_extension_group))

    ret_val += m.group(file_pattern.general_gz_extension_group) if m.group(file_pattern.general_gz_extension_group) is not None else ''

    return ret_val


def readme_filename_transform(m):
    '''Method used to transform the provided name to maven_config.metadata_index_pattern
    Arguments:
      m - the regex group match
    Returns:
      The transformed file name.
    '''
    assert m is not None

    date_level = init_time.strftime(date_level_format)
    instrument = m.group(file_pattern.general_instrument_group)[:3]
    level_name = 'pds'
    ret_val = 'mvn_{}_{}_{}_{}'.format(instrument,
                                       level_name,
                                       m.group(maven_config.meta_type_group),
                                       date_level)

    ret_val += '.{}'.format(m.group(file_pattern.general_extension_group))

    ret_val += m.group(file_pattern.general_gz_extension_group) if m.group(file_pattern.general_gz_extension_group) is not None else ''

    return ret_val


def record_filename_transform(original_bn, new_bn):
    '''Method used to log a name transform into a CSV file in /maven/mavenpro on the server
    Arguments:
        original_bn - The original base name
        new_bn - The transformed base name
    Returns:
        None
    '''
    with open(constants.filename_transforms_location, 'a') as ofile:
        writer = csv.writer(ofile)
        writer.writerow([new_bn, original_bn])
        ofile.close()


file_rules = [
    PatternDestinationRule(anc_config.radio_resid_regex,
                           None,
                           None,
                           radio_absolute_loc_generation,
                           None,
                           file_duplicate_check,
                           False),
    PatternDestinationRule(anc_config.radio_l3a_regex,
                           None,
                           None,
                           radio_absolute_loc_generation,
                           None,
                           file_duplicate_check,
                           False),
    PatternDestinationRule(anc_config.radio_data_regex,
                           None,
                           None,
                           radio_absolute_loc_generation,
                           None,
                           None,
                           True),
    PatternDestinationRule(maven_config.metadata_regex,
                           None,
                           metadata_filename_transform,
                           metadata_absolute_loc_generation,
                           record_filename_transform,
                           metadata_duplicate_check,
                           True),
    PatternDestinationRule(maven_config.sis_regex,
                           None,
                           sis_filename_transform,
                           sis_absolute_loc_generation,
                           record_filename_transform,
                           metadata_duplicate_check,
                           True),
    PatternDestinationRule(maven_config.metadata_caveats_regex,
                           None,
                           caveats_filename_transform,
                           caveats_absolute_loc_generation,
                           record_filename_transform,
                           metadata_duplicate_check,
                           True),
    PatternDestinationRule(maven_config.metadata_readme_regex,
                           None,
                           readme_filename_transform,
                           metadata_absolute_loc_generation,
                           record_filename_transform,
                           metadata_duplicate_check,
                           True),
    PatternDestinationRule(maven_config.metadata_version_changes_regex,
                           None,
                           version_changes_filename_transform,
                           metadata_absolute_loc_generation,
                           record_filename_transform,
                           metadata_duplicate_check,
                           True),
    PatternDestinationRule(maven_config.science_regex,
                           [(file_pattern.general_instrument_group, re.compile('swe')),
                            (file_pattern.general_level_group, re.compile(r'l3'))],
                           None,
                           sweal3_loc_generation,
                           None,
                           None,
                           False),
    PatternDestinationRule(maven_config.science_regex,
                           [(file_pattern.general_instrument_group, maven_config.iuv_instrument_group_regex)],
                           None,
                           iuvs_loc_generation,
                           None,
                           None,
                           False),
    PatternDestinationRule(maven_config.label_regex,
                           [(file_pattern.general_instrument_group, maven_config.iuv_instrument_group_regex)],
                           None,
                           iuvs_loc_generation,
                           None,
                           None,
                           False),
    PatternDestinationRule(maven_config.ql_regex,
                           None,
                           None,
                           ql_absolute_loc_generation,
                           None,
                           None,
                           True),
    PatternDestinationRule(maven_config.playback_file_regex,
                           None,
                           None,
                           playback_absolute_loc_generation,
                           None,
                           None,
                           True),
    PatternDestinationRule(maven_config.kp_regex,
                           None,
                           None,
                           kp_absolute_loc_generation,
                           None,
                           None,
                           True),
    PatternDestinationRule(maven_config.sep_anc_regex,
                           None,
                           None,
                           sep_anc_absolute_loc_generation,
                           None,
                           None,
                           True),
    PatternDestinationRule(maven_config.euv_l2b_regex,
                           None,
                           None,
                           euv_l2b_absolute_loc_generation,
                           None,
                           None,
                           True),
    PatternDestinationRule(maven_config.label_regex,
                           None,
                           None,
                           label_file_absolute_loc_generation,
                           None,
                           None,
                           True),
    PatternDestinationRule(maven_config.science_regex,
                           None,
                           None,
                           general_absolute_loc_generation,
                           None,
                           None,
                           True),  # catch all for general science files
    PatternDestinationRule(maven_config.euv_regex,
                           None,
                           None,
                           euv_absolute_loc_generation,
                           None,
                           None,
                           True),
    PatternDestinationRule(maven_config.euv_flare_regex,
                           None,
                           None,
                           euv_flare_absolute_loc_generation,
                           None,
                           None,
                           True),
    PatternDestinationRule(maven_config.euv_flare_catalog_regex,
                           None,
                           None,
                           euv_flare_catalog_absolute_loc_generation,
                           None,
                           None,
                           True),
    PatternDestinationRule(maven_config.euv_l4_regex,
                           None,
                           None,
                           general_absolute_loc_generation,
                           None,
                           None,
                           True),
]

# file mod time must be at least this old
# before we consider moving it
age_limit = 60.0  # one minute ago
