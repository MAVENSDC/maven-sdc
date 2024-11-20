# pylint: disable=R1702

import os
import shutil
import stat
import logging
from . import config
from maven_status import status, MAVEN_SDC_COMPONENT, MAVEN_SDC_EVENTS
from maven_utilities import file_pattern, anc_config, utilities as mvn_utils
from maven_utilities.enums import DUPLICATE_ACTION

logger = logging.getLogger('maven.ingest_spice_kernels.utilities.log')
db_logger = logging.getLogger('maven.ingest_spice_kernels.utilities.db_log')


def split_files(source_directory):
    '''Returns a list of the files that should be moved
    out of the directory where the POC puts the
    SPICE kernels.
    '''
    all_fns = os.listdir(source_directory)
    matching_fns = [fn for fn in all_fns if file_pattern.matches([anc_config.anc_spice_general_regex], fn)]
    non_matching_fns = list(set(all_fns) - set(matching_fns))
    return matching_fns, non_matching_fns


def move_copy_update_permissions(src_file, dest_file, permissions_flag, move):
    '''Moves or copies src_file to dest_loc and modifies the resulting permission to include
    the provided permissions_flag
    Arguments:
        move: false (copies), true (moves)'''
    assert os.path.isfile(src_file), "Not a file? %s" % src_file
    if os.path.isfile(dest_file):
        if mvn_utils.files_are_same(src_file,dest_file):
            logger.info('File %s already exists on the SDC.  Removing dropbox file',src_file)
            os.remove(src_file)
        else:
            raise Exception('File %s already exists and is different than dropbox' % dest_file)
    else:
        if move:
            shutil.move(src_file, dest_file)
        else:
            shutil.copy(src_file, dest_file)
        # Ensure the moved/copied file is world readable
        mode = os.stat(dest_file).st_mode
        with_permissions_flag = mode | permissions_flag
        os.chmod(dest_file, with_permissions_flag)


def ingest_spice_files(dupe_dir=None):
    '''The driving wheel. It moves the files from the source
    directory to their proper destination directories.
    '''

    dupe_dir = dupe_dir or config.dupe_dir_name
    overwritten_archived_files = []
    removed_files = []

    for _next_rule in config.file_rules:
        try:
            all_matching_fns, all_non_matching_fns = split_files(_next_rule.source_directory())
            old_matching_fns = [fn for fn in all_matching_fns if mvn_utils.file_is_old_enough(os.path.join(_next_rule.source_directory(), fn), config.age_limit)]
            old_non_matching_fns = [fn for fn in all_non_matching_fns if mvn_utils.file_is_old_enough(os.path.join(_next_rule.source_directory(), fn), config.age_limit)]
            if (len(old_non_matching_fns) > 0):
                logger.warning("There are %d invalid files in %s", len(old_non_matching_fns), _next_rule.source_directory())
                for fn in old_non_matching_fns:
                    invalid_filename = os.path.join(config.invalid_dir, fn)
                    logger.warning("moving invalid file %s", invalid_filename)
                status.add_status(component_id=MAVEN_SDC_COMPONENT.SPICE_INGESTER,
                                  event_id=MAVEN_SDC_EVENTS.FAIL,
                                  summary='There are invalid filenames in %s:' % _next_rule.source_directory(),
                                  description='Invalid files:\n' + '\n\t'.join(old_non_matching_fns))
    
            for fn in old_matching_fns:
                m = file_pattern.matches_on_group(_next_rule.patterns, fn)
                if m is not None:
                    dest_path = _next_rule.absolute_directories(m)
    
                    if dest_path is None:
                        logger.debug("Destination path %s does not exist", dest_path)
                        old_non_matching_fns.append(fn)
                        continue
    
                    dest_filename = _next_rule.filename_transform(m, dest_path)
                    src_full_filename = os.path.join(_next_rule.source_directory(), fn)
    
                    if _next_rule.copy_directory:
                        copy_path = _next_rule.copy_directory(m)
                        dest_full_file_name = os.path.join(copy_path, dest_filename)
    
                        logger.info('Copying spice file from %s to %s', src_full_filename, dest_full_file_name)
                        move_copy_update_permissions(src_full_filename, dest_full_file_name, stat.S_IROTH, False)
    
                    dest_full_file_name = os.path.join(dest_path, dest_filename)

                    if _next_rule.duplicate_check:
                        duplicate_action = _next_rule.duplicate_check(src_full_filename,dest_full_file_name)
                        if duplicate_action == DUPLICATE_ACTION.REMOVE:
                            os.remove(src_full_filename)
                            db_logger.info('Duplicate file found %s.  Remove source file',dest_path)
                            removed_files.append(src_full_filename)
                        elif duplicate_action == DUPLICATE_ACTION.OVERWRITE_ARCHIVE:
                            dup_dest_filename = os.path.join(dupe_dir, os.path.basename(dest_path))
                            # Archive existing file
                            shutil.move(dest_path, dup_dest_filename)
                            move_copy_update_permissions(src_file=src_full_filename, dest_file=dest_path,permissions_flag=stat.S_IROTH,move=True)
                            db_logger.info('Duplicate file found %s.  Archiving and overwriting dest file',dest_path)
                            overwritten_archived_files.append(dest_path)
                        elif duplicate_action == DUPLICATE_ACTION.IGNORE:
                            move_copy_update_permissions(src_file=src_full_filename, dest_file=dest_path,permissions_flag=stat.S_IROTH,move=True)
                else:
                    logger.debug("No patterns matched spice file %s", fn)
                    old_non_matching_fns.append(fn)
            for fn in old_non_matching_fns:
                src_path = os.path.join(_next_rule.source_directory(), fn)
                dest_path = os.path.join(config.invalid_dir, fn)
                move_copy_update_permissions(src_path, dest_path, stat.S_IROTH, True)

        except BaseException as e:
            err_msg = "Spice Kernel files failed to move in %s with this exception: %s" % (_next_rule.source_directory(), repr(e))
            logger.critical(err_msg)
            status.add_exception_status(component_id=MAVEN_SDC_COMPONENT.SPICE_INGESTER,
                                        event_id=MAVEN_SDC_EVENTS.FAIL,
                                        summary=err_msg)
    if len(overwritten_archived_files) > 0:
        logger.info('Overwrote/archived Files:\n' + '  \n'.join(overwritten_archived_files))
        status.add_status(component_id=MAVEN_SDC_COMPONENT.SPICE_INGESTER,
                          event_id=MAVEN_SDC_EVENTS.STATUS,
                          summary='SPICE ingester overwrote/archived duplicate files',
                          description='Overwrote/archived Files:\n' + '  \n'.join(overwritten_archived_files))
    if len(removed_files) > 0:
        logger.info('Removed Files:\n' + '  \n'.join(removed_files))
        status.add_status(component_id=MAVEN_SDC_COMPONENT.SPICE_INGESTER,
                          event_id=MAVEN_SDC_EVENTS.STATUS,
                          summary='SPICE ingester removed duplicate files',
                          description='Removed Files:\n' + '  \n'.join(removed_files))
