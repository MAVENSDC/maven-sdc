# pylint: disable=W0603
import os
import stat
import shutil
import hashlib
import logging
from subprocess import Popen, PIPE
import gzip

import maven_database
from maven_database.models import MavenDropboxMgrMove
from . import config
from maven_utilities import file_pattern, time_utilities
from maven_utilities.utilities import file_is_old_enough, is_compressed_format
from maven_status import status, MAVEN_SDC_COMPONENT, MAVEN_SDC_EVENTS

db_logger = logging.getLogger('maven.maven_dropbox_mgr.utilities.db_log')
logger = logging.getLogger('maven.maven_dropbox_mgr.utilities.log')


def find_dir_loc(search_dir, dir_name, limit_dir='/'):
    '''Method used to search for the configured invalid_directory starting at search_dir and
    working backwards up the hierarchy to limit_dir'''
    parent = current = os.path.normpath(search_dir)  # normalize relative paths
    while limit_dir in parent and current != '':
        result_dir = os.path.join(parent, dir_name)
        if config.invalid_dir_name in os.listdir(parent) and os.path.isdir(result_dir):
            return result_dir
        parent, current = os.path.split(parent)
    return None


def is_valid_dropbox_file(filename):
    '''Return True if the filename matches any of the recognized patterns'''
    bn = os.path.basename(filename)

    for next_rule in config.file_rules:
        if file_pattern.matches_on_group([next_rule.pattern], bn, next_rule.groups_to_check) is not None:
            logger.debug("%s is a valid dropbox filename", filename)
            return True

    return False


# pylint: disable=R0915,R0912
def move_valid_dropbox_file(src_filename,
                            invalid_dir,
                            dupe_dir):
    '''Moves the file from the dropbox to its home in shared storage.'''
    assert os.path.isfile(src_filename), f'{src_filename} is not a file'
    assert os.path.isdir(invalid_dir), f'The invalid directory {invalid_dir} must exist'
    assert os.path.isdir(dupe_dir), f'The duplicate directory {dupe_dir} must exist'
    
    duplicate_action_taken = None
    bn = os.path.basename(src_filename)
    for next_rule in config.file_rules:
        m = file_pattern.matches_on_group([next_rule.pattern], bn, next_rule.groups_to_check)
        if m is not None:
            if is_compressed_format(src_filename) and next_rule.unzip_compressed:
                db_logger.info('Attempting to unzip %s prior to dropbox move', src_filename)
                with gzip.GzipFile(src_filename, 'rb') as gzipfile:
                    try:
                        # Create unzipped file
                        tmp_filename = os.path.splitext(src_filename)[0]

                        with open(tmp_filename, 'wb') as tmp_file:
                            tmp_file.write(gzipfile.read())
                        # Remove zipped file from file system
                        os.remove(src_filename)

                        src_filename = tmp_filename
                        bn = os.path.basename(src_filename)
                        m = file_pattern.matches_on_group([next_rule.pattern], bn, next_rule.groups_to_check)
                    except IOError as e:
                        err = 'Unable to unzip file %s due to %s', src_filename, str(e)
                        db_logger.error(err)
                        raise RuntimeError(err)
            
            # Obtain destination directory
            # absolute loc = base_dir / dynamic_dir / bn
            base_dir, dynamic_dir = next_rule.absolute_directories(m)
            abs_dir = base_dir
            if base_dir is None:
                logger.warning('Unable to get destination directory for file %s', src_filename)
                move_invalid_file(src_filename, invalid_dir)
                return None
            if dynamic_dir is not None:
                abs_dir = os.path.join(base_dir, dynamic_dir)

            # Handle dynamic directory creation
            if not os.path.isdir(abs_dir):
                if not os.path.isdir(base_dir):
                    raise RuntimeError("The base directory %s doesn't exist!  Unable to move file %s" % (base_dir, src_filename))
                os.makedirs(abs_dir, 0o755)
                os.chmod(os.path.split(abs_dir)[0], 0o755)  # Year directory
                os.chmod(abs_dir, 0o755)  # Month directory
                
            # generate file name
            dest_file_basename = src_filename
            if next_rule.filename_transform is not None:
                dest_file_basename = next_rule.filename_transform(m)
                db_logger.info('DBM transformed %s into %s', src_filename, dest_file_basename)
            dest_filename = os.path.join(abs_dir, os.path.basename(dest_file_basename))
                        
            # Record any file name transforms
            if next_rule.transform_record_keeping is not None:
                next_rule.transform_record_keeping(bn, dest_file_basename)
            
            # Handle duplicates
            if next_rule.duplicate_check is not None:
                action, discovered_dest_fname = next_rule.duplicate_check(src_filename, dest_filename)
                duplicate_action_taken = action
                if action == config.DUPLICATE_ACTION.REMOVE:
                    db_logger.info('Duplicate file found in %s, removing %s', abs_dir, src_filename)
                    os.remove(src_filename)
                    return None
            
            # Record any file name transforms
            if next_rule.transform_record_keeping is not None:
                next_rule.transform_record_keeping(bn, dest_file_basename)
            
            if next_rule.duplicate_check is not None:
                action, discovered_dest_fname = next_rule.duplicate_check(src_filename, dest_filename)
                duplicate_action_taken = action
                if action == config.DUPLICATE_ACTION.REMOVE:
                    db_logger.info('Duplicate file found in %s, removing %s', abs_dir, src_filename)
                    os.remove(src_filename)
                    return duplicate_action_taken
                if action == config.DUPLICATE_ACTION.OVERWRITE: 
                    db_logger.info('Overwriting destination file %s', dest_filename)
                    os.remove(discovered_dest_fname)
                elif action == config.DUPLICATE_ACTION.OVERWRITE_ARCHIVE:
                    dup_dest_filename = os.path.join(dupe_dir, os.path.basename(discovered_dest_fname))
                    db_logger.info('Archiving duplicate file %s to %s', discovered_dest_fname, dup_dest_filename)
                    shutil.move(discovered_dest_fname, dup_dest_filename)
                elif action == config.DUPLICATE_ACTION.ARCHIVE:
                    dup_dest_filename = os.path.join(dupe_dir, os.path.basename(src_filename))
                    db_logger.info('Moving duplicate file %s to %s', src_filename, dup_dest_filename)
                    shutil.move(src_filename, dup_dest_filename)
                elif action == config.DUPLICATE_ACTION.IGNORE:
                    pass
            
            shutil.move(src_filename, dest_filename)
            mode = os.stat(dest_filename).st_mode
            with_world_readable_mode = mode | stat.S_IROTH
            os.chmod(dest_filename, with_world_readable_mode)
            log_move_in_db(src_filename, dest_filename)
            return duplicate_action_taken

    raise RuntimeError("Unable to move valid file %s.  No rule matched the base filename %s" % (src_filename, bn))


def move_invalid_file(src_filename, invalid_dir):
    '''Moves the file from the dropbox to its invalid home in shared storage.'''
    assert os.path.isfile(src_filename), '%s is not a file' % src_filename
    _, name = os.path.split(src_filename)

    invalid_filename = os.path.join(invalid_dir, name)
    shutil.move(src_filename, invalid_filename)
    logger.warning("moving invalid file %s", invalid_filename)


def log_move_in_db(src_filename, dest_filename):
    '''Inserts information about the move in the database.'''
    utcnow = time_utilities.utc_now()
    md5 = hashlib.md5(open(dest_filename, 'rb').read()).hexdigest()
    file_size = os.path.getsize(dest_filename)
    m = MavenDropboxMgrMove(utcnow, src_filename, dest_filename, md5, file_size)
    maven_database.db_session.add(m)
    maven_database.db_session.commit()


def make_file_list(root_directory):
    '''Make a list of filenames for all the files
        in root_directory.'''
    filenames = []
    for path, _, files in os.walk(root_directory):
        for f in files:
            fn = os.path.join(path, f)
            if os.path.isfile(fn):
                filenames.append(fn)
    return filenames


def file_is_open(path):
    '''Returns true if the file has been opened by any process, using
    the linux lsof utility.'''
    p = Popen(['lsof', '-t', path], stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    logger.debug('lsof result %s, "%s"', p.returncode, output.replace(b'\n', b''))
    # note lsof returns exit code 1 if no one has the file open
    return p.returncode == 0 and len(output) > 0 and len(error) == 0


def file_is_stable(path):
    '''Returns true if the file is old enough and
        no other process has it open.'''
    if not file_is_old_enough(path, config.age_limit):
        logger.info("%s is not older than %d", path, config.age_limit)
        return False
    # this shouldn't be called too frequently
    if file_is_open(path):
        logger.debug("%s is already open", path)
        return False
    # check age once more after file_is_open returns False
    return file_is_old_enough(path, config.age_limit)


def move_files_in_directory_tree(root_directory):
    '''Moves the files in a directory tree.

    Argument
        root_directory - root of the directory tree that holds the files in the dropbox
    '''

    invalid_dir = find_dir_loc(root_directory,
                               config.invalid_dir_name,
                               os.path.normpath(os.path.join(root_directory, '..')))

    if invalid_dir is None:
        invalid_dir = os.path.join(root_directory, '..', config.invalid_dir_name)
        logger.info("creating invalid directory %s", invalid_dir)
        os.makedirs(invalid_dir)
    
    duplicate_dir = find_dir_loc(root_directory,
                                 config.dupe_dir_name,
                                 os.path.normpath(os.path.join(root_directory, '..')))

    if duplicate_dir is None:
        duplicate_dir = os.path.join(root_directory, '..', config.dupe_dir_name)
        logger.info("creating duplicate directory %s", duplicate_dir)
        os.makedirs(duplicate_dir)

    try:
        # get a list of all files in root_directory
        all_filenames = make_file_list(root_directory)

        # get filenames that are old enough and are not opened by another process
        filenames = [fn for fn in all_filenames if file_is_stable(fn)]

        valid_filenames = [fn for fn in filenames if is_valid_dropbox_file(fn)]

        # Collect list of invalid files
        invalid_files = [fn for fn in filenames if not is_valid_dropbox_file(fn)]

        overwritten_files = []
        overwritten_archived_files = [] 
        removed_files = [] 
        archived_files = []
        # move the stable files
        for fn in valid_filenames:
            duplicate_action = move_valid_dropbox_file(fn, invalid_dir, duplicate_dir)
            
            if duplicate_action == config.DUPLICATE_ACTION.OVERWRITE:
                overwritten_files.append(fn)
            elif duplicate_action == config.DUPLICATE_ACTION.OVERWRITE_ARCHIVE:
                overwritten_archived_files.append(fn)
            elif duplicate_action == config.DUPLICATE_ACTION.REMOVE:
                removed_files.append(fn)
            elif duplicate_action == config.DUPLICATE_ACTION.ARCHIVE:
                archived_files.append(fn)
        
        if len(overwritten_files) > 0:
            logger.info('Overwrote Files:\n' + '  \n'.join(overwritten_files))
            status.add_status(component_id=MAVEN_SDC_COMPONENT.DROPBOX,
                              event_id=MAVEN_SDC_EVENTS.STATUS,
                              summary='Dropbox overwrote duplicate files',
                              description='Overwrote Files:\n' + '  \n'.join(overwritten_files))
        if len(overwritten_archived_files) > 0:
            logger.info('Overwrote/archived Files:\n' + '  \n'.join(overwritten_archived_files))
            status.add_status(component_id=MAVEN_SDC_COMPONENT.DROPBOX,
                              event_id=MAVEN_SDC_EVENTS.STATUS,
                              summary='Dropbox overwrote/archived duplicate files',
                              description='Overwrote/archived Files:\n' + '  \n'.join(overwritten_archived_files))
        if len(removed_files) > 0:
            logger.info('Removed Files:\n' + '  \n'.join(removed_files))
            status.add_status(component_id=MAVEN_SDC_COMPONENT.DROPBOX,
                              event_id=MAVEN_SDC_EVENTS.STATUS,
                              summary='Dropbox removed duplicate files',
                              description='Removed Files:\n' + '  \n'.join(removed_files))
        if len(archived_files) > 0:
            logger.info('Archived Files:\n' + '  \n'.join(archived_files))
            status.add_status(component_id=MAVEN_SDC_COMPONENT.DROPBOX,
                              event_id=MAVEN_SDC_EVENTS.STATUS,
                              summary='Dropbox archived duplicate files',
                              description='Archived Files:\n' + '  \n'.join(archived_files))

        # report and move invalid files
        if len(invalid_files) > 0:
            db_logger.warning('There are %d invalid files in %s.', len(invalid_files), root_directory)
            for fn in invalid_files:
                db_logger.warning('Invalid file: %s', fn)

            status.add_status(component_id=MAVEN_SDC_COMPONENT.DROPBOX,
                              event_id=MAVEN_SDC_EVENTS.FAIL,
                              summary='There are invalid filenames in %s:\n' % root_directory,
                              description='Invalid Files:\n' + '  \n'.join(invalid_files))
            for fn in invalid_files:
                move_invalid_file(fn, invalid_dir)

    except BaseException as e:
        err_msg = "The dropbox manager failed to move files in %s with this exception: %s" % (root_directory, str(e))
        db_logger.critical(err_msg)
        status.add_exception_status(component_id=MAVEN_SDC_COMPONENT.DROPBOX,
                                    event_id=MAVEN_SDC_EVENTS.FAIL,
                                    summary=err_msg)
