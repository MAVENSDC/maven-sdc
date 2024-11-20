import os
import logging
from collections import namedtuple
from functools import cmp_to_key
from maven_utilities import anc_config, maven_config
from maven_utilities.utilities import listdir_files, get_file_root_plus_extension_with_version_and_revision, files_are_same
from maven_utilities import file_pattern
from maven_utilities.enums import DUPLICATE_ACTION

logger = logging.getLogger('maven.ingest_spice_kernels.config.log')

invalid_dir = '/maven/data/sdc/misnamed_files/'
poc_outbox = '/maven/poc/outbox/'
poc_root_source_dir = '/maven/poc/spice/'
iuv_root_source_dir = '/maven/itfhome/iuvs/spice_dropbox/'
poc_root_dest_dir = '/maven/data/anc/spice/'
dupe_dir_name = 'duplicate_files'
age_limit = 600.0  # 10 minutes ago

PatternDestinationRule = namedtuple('PatternDestinationRule', ['patterns',
                                                               'absolute_directories',
                                                               'filename_transform',
                                                               'copy_directory',
                                                               'source_directory',
                                                               'duplicate_check'
                                                               ])


def get_basename(filename):
    '''Returns the base of the filename, whether the file has a
    version number or not
    '''
    m = file_pattern.extract_parts([maven_config.root_verrev_ext_regex],
                                   filename,
                                   parts=[file_pattern.general_root_group])
    assert m is not None, "Bad filename (basename) %s" % filename
    root, = m.values()
    return root


def filename_has_version(filename):
    '''Returns True if there is a version in the name of the file.'''
    m = file_pattern.extract_parts([maven_config.root_verrev_ext_regex],
                                   filename,
                                   parts=[file_pattern.general_version_group])
    return m[file_pattern.general_version_group] != None


def cmp_versions(filename_1, filename_2):
    '''Compares two files based on their version.'''
    
    def _cmp(a, b):
        return (a > b) - (a < b)
    
    f1, v1, _ = get_file_root_plus_extension_with_version_and_revision(filename_1)
    assert f1 is not None, "Bad filename_1 (cmp_versions) %s" % filename_1
    f2, v2, _ = get_file_root_plus_extension_with_version_and_revision(filename_2)
    assert f2 is not None, "Bad filename_2 (cmp_versions) %s" % filename_2
    return _cmp(v1, v2)


def matches_basename(basename, filename):
    '''Returns True if the filename has the same base as the specified basename.'''
    return get_basename(filename) == basename


def increment_version(filename):
    '''Returns a filename with the version incremented by one.'''
    m = file_pattern.matches_on_group([maven_config.root_verrev_ext_regex], filename)
    assert m, "Bad filename %s" % filename
    basename, version_str, _, extension = m.groups()
    new_version = int(version_str) + 1
    return '%s_v%03d.%s' % (basename, new_version, extension)


def get_matching_dest_filename(dest_directory, filename):
    '''Returns the destination filename that matches the src filename and has the largest
    version.
    '''
    basename = get_basename(filename)
    dest_filenames = listdir_files(dest_directory)
    matching_filenames = [fn for fn in dest_filenames if matches_basename(basename, fn)]
    assert len(matching_filenames) > 0, "No matching filenames in dest %s for filename %s" % (dest_directory, filename)
    sorted_matching_filenames = sorted(matching_filenames, key=cmp_to_key(cmp_versions))
    return sorted_matching_filenames[-1]


def filename_exists_in_destination_directory(dest_directory, filename):
    '''Returns True if there is a file that matches the filename in the
    destination directory.
    '''
    basename = get_basename(filename)
    for fn in listdir_files(dest_directory):
        dir_file_basename = get_basename(fn)
        if dir_file_basename == basename:
            logger.debug("%s matches filename in destination directory %s", filename, dest_directory)
            return True
    logger.debug("%s does NOT match filename in destination directory %s", filename, dest_directory)
    return False


def get_bc_destination_filename_with_new_version(source_directory, dest_directory, bc_filename):
    '''Returns a destination filename with a new version

    Takes care of versioning of the file.
    '''
    if filename_exists_in_destination_directory(dest_directory, bc_filename):
        logger.debug("%s exists in destination directory %s", bc_filename, dest_directory)
        latest_existing_filename = get_matching_dest_filename(dest_directory, bc_filename)
        if not files_are_same(os.path.join(dest_directory, latest_existing_filename),
                              os.path.join(source_directory, bc_filename)):
            logger.debug("Contents of %s are different than %s", latest_existing_filename, bc_filename)
            return increment_version(latest_existing_filename)
        
        logger.debug("Contents of %s == %s", latest_existing_filename, bc_filename)
        return latest_existing_filename

    basename = get_basename(bc_filename)
    versioned_basename = '%s_v001.bc' % basename
    logger.debug("%s, version %s, does not exist in destination directory %s", bc_filename, versioned_basename, dest_directory)
    return versioned_basename


def get_versioned_bc_filename(source_directory, dest_directory, bc_filename):
    '''Returns the filename for the specified bc kernel file.

    Takes care of versioning of the file if need be.
    '''
    if filename_has_version(bc_filename):
        # Don't change the version, just keep the one that is already in the filename.
        logger.debug("%s includes a version", bc_filename)
        return bc_filename
    logger.debug("%s does NOT include a version", bc_filename)
    return get_bc_destination_filename_with_new_version(source_directory, dest_directory, bc_filename)


def file_duplicate_check(src_fn, dst_fn):
    '''Method used to determine if source and destination files match and suggest an action
    Arguments:
        src_fn - the full name and path of the file you want to check for duplicates of
        dest_fn - the full name and path where src_fn will be moved
    '''
    if not os.path.isfile(src_fn) or not os.path.isfile(dst_fn):
        return DUPLICATE_ACTION.IGNORE
    if os.path.basename(src_fn) == os.path.basename(dst_fn):
        if files_are_same(src_fn, dst_fn):
            return DUPLICATE_ACTION.REMOVE
        return DUPLICATE_ACTION.OVERWRITE_ARCHIVE
    return DUPLICATE_ACTION.IGNORE

def anc_spice_general_loc_generation(m):
    '''Method used to generate a directory location for the ancillary SPICE files
    Arguments:
        m - Regex match results
    Returns:
        The resulting destination directory
    '''
    assert m is not None

    file_type = m.group(file_pattern.general_extension_group)
    if file_type == 'tls':
        return_path = os.path.join(poc_root_dest_dir, 'lsk')
    elif file_type == 'tsc':
        return_path = os.path.join(poc_root_dest_dir, 'sclk')
    elif file_type == 'tpc':
        return_path = os.path.join(poc_root_dest_dir, 'pck')
    elif file_type == 'bsp':
        return_path = os.path.join(poc_root_dest_dir, 'spk')
    elif file_type == 'bc':
        return_path = os.path.join(poc_root_dest_dir, 'ck')
    elif file_type == 'ti':
        return_path = os.path.join(poc_root_dest_dir, 'ik')
    elif file_type == 'tf':
        return_path = os.path.join(poc_root_dest_dir, 'fk')
    else:
        return_path = None
        logger.debug("file_type %s does not exist", file_type)
    logger.debug("file_type %s with a return path %s", file_type, return_path)
    return return_path


file_rules = [
    PatternDestinationRule([anc_config.anc_iuv_all_regex, anc_config.anc_iuv_all_one_month_regex],
                           lambda m: os.path.join(poc_root_dest_dir + '/ck'),
                           lambda m, dest_dir:get_versioned_bc_filename(iuv_root_source_dir, dest_dir, m.group(0)),
                           lambda m: poc_outbox,
                           lambda: iuv_root_source_dir,
                           None),
    PatternDestinationRule([anc_config.anc_spice_general_regex],
                           anc_spice_general_loc_generation,
                           lambda m, _: m.group(0),
                           None,
                           lambda: poc_root_source_dir,
                           file_duplicate_check),
]
