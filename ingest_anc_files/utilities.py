import os
import stat
import shutil
import logging
from . import config
from maven_utilities import file_pattern
from maven_utilities.enums import DUPLICATE_ACTION
from maven_utilities.utilities import file_is_old_enough, files_are_same
from maven_status import status, MAVEN_SDC_COMPONENT, MAVEN_SDC_EVENTS

logger = logging.getLogger('maven.ingest_anc_files.utilities.log')
db_logger = logging.getLogger('maven.ingest_anc_files.utilities.db_log')


def split_files(files_list):
    good_files = [fn for fn in files_list if file_pattern.matches(config.all_patterns, fn)]

    bad_files = list(set(files_list) - set(good_files))

    return good_files, bad_files


def move_invalid_file(src_filename, base_directory):

    fully_qualified_filename = os.path.join(base_directory, src_filename)
    assert os.path.isfile(fully_qualified_filename), '%s is not a file' % fully_qualified_filename

    invalid_filename = os.path.join(config.invalid_dir, src_filename)
    shutil.move(fully_qualified_filename, invalid_filename)
    mode = os.stat(invalid_filename).st_mode
    with_world_readable_mode = mode | stat.S_IROTH
    logger.warning("moving invalid file %s", invalid_filename)
    os.chmod(invalid_filename, with_world_readable_mode)


def get_destination_path(filename, dest_root_directory):
    '''Returns the full path to where the file should be moved.'''
    for _next in config.file_rules:
        for rule_pattern in _next.patterns:
            next_match = rule_pattern.match(filename)
            if next_match:
                destination_path = os.path.join(_next.absolute_directory(next_match, dest_root_directory), filename)
                logger.debug("Resulting anc destination path: %s", destination_path)
                return destination_path
    raise Exception('ancillary file %s name did not match any pattern' % filename)


def get_src_file_names(source_directory):
    '''Returns a list of the files that should be moved
    out of the directory where the POC puts the
    ancillary data.
    '''
    fns = os.listdir(source_directory)
    return fns


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
    return DUPLICATE_ACTION


def _move_and_chmod(src,dst):
    shutil.move(src, dst)
    mode = os.stat(dst).st_mode
    with_world_readable_mode = mode | stat.S_IROTH
    os.chmod(dst, with_world_readable_mode)

def ingest_anc_files(src_directory,
                     dest_root_directory,
                     dupe_dir=None):
    '''The driving wheel. It moves the files from the source
    directory to their proper destination directories.
    '''
    dupe_dir = dupe_dir or config.dupe_dir_name
    try:
        all_fns = get_src_file_names(src_directory)
        correct_fns, incorrect_fns = split_files(all_fns)
        ready_correct_fns = [fn for fn in correct_fns if file_is_old_enough(os.path.join(src_directory, fn), config.age_limit)]
        ready_incorrect_fns = [fn for fn in incorrect_fns if file_is_old_enough(os.path.join(src_directory, fn), config.age_limit)]

        overwritten_archived_files = []
        removed_files = []

        if (len(ready_incorrect_fns) > 0):
            logger.warning("There are %d invalid files in %s", len(ready_incorrect_fns), src_directory)
            for fn in ready_incorrect_fns:
                logger.warning("Invalid file: %s", fn)
            status.add_status(component_id=MAVEN_SDC_COMPONENT.ANC_INGESTER,
                              event_id=MAVEN_SDC_EVENTS.FAIL,
                              summary='There are invalid filenames in %s:' % src_directory,
                              description='Invalid files:\n' + '\n\t'.join(ready_incorrect_fns))

        for fn in ready_correct_fns:
            src_path = os.path.join(src_directory, fn)
            dest_path = get_destination_path(fn, dest_root_directory)

            duplicate_action = file_duplicate_check(src_path,dest_path)

            if duplicate_action == DUPLICATE_ACTION.REMOVE:
                os.remove(src_path)
                db_logger.info('Duplicate file found %s.  Remove source file',dest_path)
                removed_files.append(src_path)
            elif duplicate_action == DUPLICATE_ACTION.OVERWRITE_ARCHIVE:
                dup_dest_filename = os.path.join(dupe_dir, os.path.basename(dest_path))
                #Archive existing file
                shutil.move(dest_path, dup_dest_filename)
                _move_and_chmod(src=src_path,dst=dest_path)
                db_logger.info('Duplicate file found %s.  Archiving and overwriting dest file',dest_path)
                overwritten_archived_files.append(dest_path)
            elif duplicate_action == DUPLICATE_ACTION.IGNORE:
                _move_and_chmod(src=src_path,dst=dest_path)

        for fn in ready_incorrect_fns:
            move_invalid_file(fn, src_directory)

        #Status
        if len(overwritten_archived_files) > 0:
            logger.info('Overwrote/archived Files:\n' + '  \n'.join(overwritten_archived_files))
            status.add_status(component_id=MAVEN_SDC_COMPONENT.ANC_INGESTER,
                              event_id=MAVEN_SDC_EVENTS.STATUS,
                              summary='Ancillary Ingester overwrote/archived duplicate files',
                              description='Overwrote/archived Files:\n' + '  \n'.join(overwritten_archived_files))
        if len(removed_files) > 0:
            logger.info('Removed Files:\n' + '  \n'.join(removed_files))
            status.add_status(component_id=MAVEN_SDC_COMPONENT.ANC_INGESTER,
                              event_id=MAVEN_SDC_EVENTS.STATUS,
                              summary='Ancillary Ingester removed duplicate files',
                              description='Removed Files:\n' + '  \n'.join(removed_files))

    except BaseException as e:
        err_msg = "Ancillary files failed to move in %s with this exception: %s" % (src_directory, str(e))
        logger.critical(err_msg)
        status.add_exception_status(component_id=MAVEN_SDC_COMPONENT.ANC_INGESTER,
                                    event_id=MAVEN_SDC_EVENTS.FAIL,
                                    summary=err_msg)
