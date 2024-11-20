#! /usr/bin/env python
#
# Mike Dorey  2012-11-12

'''A script that moves the MAVEN level 0 files deposited by the POC into /maven/poc
into the instrument directories /maven/data/sci/<instrument>/l0.
'''
import os
import shutil
import stat
import logging
from . import config
from maven_utilities import file_pattern, maven_config
from maven_utilities.enums import DUPLICATE_ACTION
from maven_utilities.utilities import file_is_old_enough, listdir_files, files_are_same
from maven_status import status, MAVEN_SDC_COMPONENT, MAVEN_SDC_EVENTS
from functools import cmp_to_key

logger = logging.getLogger('maven.ingest_l0_files.utilities.log')
db_logger = logging.getLogger('maven.ingest_l0_files.utilities.db_log')


def get_instrument_name(filename):
    '''Returns the name of the instrument that is embedded in the filename.

    Argument
        filename - mvn_<instrument>_l0_<YYYY><MM><DD>.dat
    '''
    m = file_pattern.extract_parts([maven_config.l0_regex],
                                   filename,
                                   parts=[file_pattern.general_instrument_group])
    assert m is not None
    inst, = m.values()
    return inst


def move_invalid_file(src_filename, base_directory):

    fully_qualified_filename = os.path.join(base_directory, src_filename)
    assert os.path.isfile(fully_qualified_filename), '%s is not a file' % fully_qualified_filename

    invalid_filename = os.path.join(config.invalid_dir, src_filename)
    shutil.move(fully_qualified_filename, invalid_filename)
    mode = os.stat(invalid_filename).st_mode
    with_world_readable_mode = mode | stat.S_IROTH
    os.chmod(invalid_filename, with_world_readable_mode)
    logger.warning("moving invalid file %s", invalid_filename)


def file_duplicate_check(src_fn, dst_fn):
    '''Method used to determine if source and destination files match and suggest an action
    Arguments:
        src_fn - the full name and path of the file you want to check for duplicates of
        dest_fn - the full name and path where src_fn will be moved
    '''
    if not os.path.isfile(src_fn) or not os.path.isfile(dst_fn):
        return DUPLICATE_ACTION.IGNORE

    if files_are_same(src_fn, dst_fn):
        return DUPLICATE_ACTION.REMOVE
    return DUPLICATE_ACTION.UP_VERSION


def _move_and_chmod(src,dst):
    shutil.move(src, dst)
    mode = os.stat(dst).st_mode
    with_world_readable_mode = mode | stat.S_IROTH
    os.chmod(dst, with_world_readable_mode)

def move_instrument_files(instrument, src_directory, dest_directory):
    '''Moves the MAVEN instrument files from the src_directory to the
    dest_directory.
    '''
    all_src_filenames = listdir_files(src_directory)
    correctly_formatted_src_filenames, _ = split_files(maven_config.l0_regex, all_src_filenames)

    ready_correctly_formatted_src_filenames = [fn for fn in correctly_formatted_src_filenames if file_is_old_enough(os.path.join(src_directory, fn), config.age_limit)]

    ''' Move correctly formatted files '''
    instrument_filenames = [fn for fn in ready_correctly_formatted_src_filenames if is_instrument_filename(instrument, fn)]

    removed_files = []

    for fn in instrument_filenames:
        if not filename_exists_in_destination_directory(fn, dest_directory):
            logger.debug("adding_v001 to %s that does not exist in destination directory %s", fn, dest_directory)
            src = os.path.join(src_directory, fn)
            dest_fn = fn.replace('.dat', '_v001.dat')
            dest = os.path.join(dest_directory, dest_fn)
            _move_and_chmod(src=src,dst=dest)
        else:
            src_path = os.path.join(src_directory, fn)
            dest_fn = get_matching_dest_filename(fn, dest_directory)
            dest_path = os.path.join(dest_directory, dest_fn)

            duplicate_action = file_duplicate_check(src_fn=src_path,
                                                    dst_fn=dest_path)

            if duplicate_action == DUPLICATE_ACTION.REMOVE:
                os.remove(src_path)
                db_logger.info('Duplicate file found %s.  Remove source file',dest_path)
                removed_files.append(src_path)
            elif duplicate_action == DUPLICATE_ACTION.UP_VERSION:
                up_version_dest_fn = increment_version(dest_fn)
                logger.debug("File paths %s and %s are different, increasing version to %s", src_path, dest_path, up_version_dest_fn)
                dest_path = os.path.join(dest_directory, up_version_dest_fn)
                _move_and_chmod(src=src_path,dst=dest_path)
            else:
                raise Exception(f'Uanble to ignore L0 file {src_path}')


def is_instrument_filename(instrument, filename):
    '''Returns True if the file belongs to the specified instrument.'''
    m = file_pattern.extract_parts([maven_config.l0_regex],

                                   filename,
                                   [file_pattern.general_instrument_group])
    if m is None:
        logger.info("%s not a part of instrument %s", filename, instrument)
        return False
    fn_inst, = m.values()
    return fn_inst == instrument


def filename_exists_in_destination_directory(filename, dest_directory):
    '''Returns True if there is a file that matches the filename in the
    destination directory.
    '''
    m = file_pattern.extract_parts([maven_config.l0_regex],
                                   filename,
                                   [file_pattern.general_basename_group])
    assert m is not None
    basename = list(m.values())
    dest_filenames = listdir_files(dest_directory)
    for fn in dest_filenames:
        m = file_pattern.extract_parts([maven_config.l0_regex],
                                       fn,
                                       parts=[maven_config.l0_base_group],
                                       transforms=file_pattern.ver_rev_transforms)
        if m is not None:
            dest_base = list(m.values())
            if dest_base == basename:
                logger.debug("%s matches filename in destination directory %s", filename, dest_directory)
                return True
    logger.debug("%s does NOT match filename in destination directory %s", filename, dest_directory)
    return False


def increment_version(filename):
    '''Returns a filename with the version incremented by one.'''
    m = file_pattern.extract_parts([maven_config.l0_regex],
                                   filename,
                                   parts=[maven_config.l0_base_group, file_pattern.general_version_group],
                                   transforms=file_pattern.ver_rev_transforms)
    assert m is not None
    basename, version_str = m.values()
    new_version = int(version_str) + 1
    return '%s_v%03d.dat' % (basename, new_version)


def get_matching_dest_filename(src_filename, dest_directory):
    '''Returns the destination filename that matches the src filename and has the largest
    version.
    '''
    m = file_pattern.matches_on_group([maven_config.l0_regex],
                                      src_filename)
    assert m is not None
    dest_filenames = listdir_files(dest_directory)
    matching_filenames = [fn for fn in dest_filenames if matches_src_filename(src_filename, fn)]
    assert len(matching_filenames) > 0
    sorted_matching_filenames = sorted(matching_filenames, key=cmp_to_key(cmp_versions))
    logger.debug("sorted matched src filename: %s", sorted_matching_filenames[-1])
    return sorted_matching_filenames[-1]


def matches_src_filename(src_filename, dest_filename):
    '''Returns True if the dest filename matches the src filename.'''
    m = file_pattern.extract_parts([maven_config.l0_regex],
                                   src_filename,
                                   parts=[maven_config.l0_base_group])
    assert m is not None
    src_m, = m.values()
    m = file_pattern.extract_parts([maven_config.l0_regex],
                                   dest_filename,
                                   parts=[maven_config.l0_base_group])
    assert m is not None
    dest_m, = m.values()
    return src_m == dest_m


def cmp_versions(fn1, fn2):

    def _cmp(a, b):
        return (a > b) - (a < b)

    '''Compares two filenames based on their versions.'''
    return _cmp(get_version(fn1), get_version(fn2))


def get_version(filename):
    '''Returns the version that is in the filename.'''
    m = file_pattern.extract_parts([maven_config.l0_regex],
                                   filename,
                                   transforms=file_pattern.ver_rev_transforms,
                                   parts=[file_pattern.general_version_group])
    assert m is not None
    ver, = m.values()
    return ver


def split_files(split_regex, files_list):
    good_files = [fn for fn in files_list if split_regex.match(fn)]
    bad_files = [fn for fn in files_list if not split_regex.match(fn)]

    return good_files, bad_files


def move_files(src_directory, dest_root_directory):
    '''The driving wheel. Moves files from the src directory to the proper destination directory.

    Arguments
        src_directory - e.g. /maven/poc
        dest_root_directory - e.g. /maven/data/sci
    '''
    try:
        src_fns = listdir_files(src_directory)
        correctly_formatted_src_filenames, incorrectly_formatted_src_filenames = split_files(maven_config.l0_regex, src_fns)
        ready_incorrectly_formatted_src_filenames = [fn for fn in incorrectly_formatted_src_filenames if file_is_old_enough(os.path.join(src_directory, fn), config.age_limit)]

        if (len(ready_incorrectly_formatted_src_filenames) > 0):
            logger.warning("There are %d invalid files in %s", len(ready_incorrectly_formatted_src_filenames), src_directory)
            for fn in ready_incorrectly_formatted_src_filenames:
                logger.warning("Invalid file: %s", fn)
            status.add_status(component_id=MAVEN_SDC_COMPONENT.L0_INGESTER,
                              event_id=MAVEN_SDC_EVENTS.FAIL,
                              summary='There are invalid filenames in %s:' % src_directory,
                              description='Invalid files:\n' + '\n\t'.join(ready_incorrectly_formatted_src_filenames))

        instruments = []
        for fn in correctly_formatted_src_filenames:
            inst = get_instrument_name(fn)
            instruments.append(inst)
        instruments = sorted(list(set(instruments)))  # get rid of duplicates
        for instrument in instruments:
            dest_directory = '%s/l0' % instrument
            dest_directory = os.path.join(dest_root_directory, dest_directory)
            assert(os.path.isdir(dest_directory))  # it has to exist before the move
            move_instrument_files(instrument, src_directory, dest_directory)

        ''' Move incorrectly formatted files '''
        for fn in ready_incorrectly_formatted_src_filenames:
            move_invalid_file(fn, src_directory)

    except BaseException as e:
        err_msg = "L0 files failed to move in %s with this exception: %s" % (src_directory, str(e))
        logger.exception(err_msg)
        status.add_exception_status(component_id=MAVEN_SDC_COMPONENT.L0_INGESTER,
                                    event_id=MAVEN_SDC_EVENTS.FAIL,
                                    summary=err_msg)
