#! /usr/bin/env python
#
# A script to read the MAVEN in-situ key parameter files
# and insert their data into the database.
#
# Mike Dorey  2013-06-12
# pylint: disable=W0603
import logging
import os
from collections import namedtuple
from contextlib import contextmanager
from multiprocessing import Pool, Lock, current_process
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import and_

from maven_database import db_session, models
from maven_database.database import engine
from maven_database.database import config as proc_config
from maven_utilities import file_pattern, maven_config, constants, time_utilities
from maven_utilities.log_config import log_path
from maven_utilities.utilities import get_file_root_plus_extension_with_version_and_revision
from .in_situ_kp_file_processor import add_kp_files_metadata_entry, insitu_file_processor
from .in_situ_progress import InSituIngestProgress
from . import config
from maven_status import status, MAVEN_SDC_COMPONENT, MAVEN_SDC_EVENTS

logger = logging.getLogger('maven.maven_in_situ_kp_file_ingester.utilities.db_log')

KpFilesStatus = namedtuple('KpFilesStatus', 'new updated complete deprecated started')
FullNameVersionRevision = namedtuple('FullNameVersionRevision', 'fully_qualified_name version revision')

proc_lock = None

@contextmanager
def create_logger(logger_name):
    local_logger = logging.getLogger(logger_name)
    if os.environ[constants.python_env] == 'testing':
        handler = logging.NullHandler()
    else:
        handler = logging.FileHandler(os.path.join(log_path, logger_name))
        handler.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s - %(name)s: '
                                              '%(levelname)s L%(lineno)d %(message)s'))
    local_logger.addHandler(handler)
    local_logger.setLevel(logging.DEBUG)
    try:
        yield local_logger
    finally:
        handler.close()
        local_logger.removeHandler(handler)


def is_in_situ_kp_file(file_path):
    '''Returns True if the file is a MAVEN
    in-situ key parameter file.

    Argument
        file_path - full path to a file
    '''
    bn = os.path.basename(file_path)
    return file_pattern.matches_on_group([maven_config.kp_regex],
                                         bn,
                                         [(file_pattern.general_level_group, maven_config.insitu_level_group_regex)])


def has_been_ingested(file_path):
    '''Returns True if the file has already been ingested.'''
    fn = os.path.basename(file_path)
    return db_session.query(models.KpFilesMetadata).filter(and_(
        models.KpFilesMetadata.file_name == fn,
        models.KpFilesMetadata.ingest_status == "COMPLETE")).count() > 0


def get_kp_files_status(root_dir):
    '''Method used to get the all the version/revision status for the files on disk
    Arguments :
        root_dir - The root direction location to find insitu kp files (recursively)
    Returns:
        {root => [ FullNameVersionRevision(fullname,version,revision) ]}
    '''
    logger.debug("Getting KP file status for directory %s", root_dir)
    root_versions_revisions = {}
    for path, _, files in os.walk(root_dir):
        for f in files:
            fn = os.path.join(path, f)
            if os.path.isfile(fn) and is_in_situ_kp_file(fn):
                _, tail = os.path.split(fn)
                root, version, revision = get_file_root_plus_extension_with_version_and_revision(tail)
                root_versions_revisions.setdefault(root, []).append(FullNameVersionRevision(fn, version, revision))
                logger.debug("Found KP file:\n\tName: %s\n\tRoot: %s\n\t Version: %d\n\t Revision: %d\n\t", fn, root, version, revision)
    return root_versions_revisions


def get_insitu_kp_files_status(root_dir):
    '''Method that retrieves the latest insitu metadata from the ingest database and compares
    the version/revision information with the version/revision information found from the files
    in root_dir.
    Argument :
        root_dir - The root direction location to find insitu kp files (recursively)
    Returns:
        KpFilesStatus - The fully qualified files names and metadata from the database if metadata exists
        for all kp insitu files
        e.g
        (
          new        [ (fullname,None) ],
          updated    [ (fullname,KpFilesMetadata) ],
          complete   [ (fullname,KpFilesMetadata) ],
          deprecated [ (fullname,KpFilesMetadata) ],
          started    [ (fullname,KpFilesMetadata) ]
        (
    '''
    results = KpFilesStatus([], [], [], [], [])
    # get current status
    current_kp_status = db_session.query(models.KpFilesMetadata).filter((models.KpFilesMetadata.file_type == 'in-situ')).order_by(models.KpFilesMetadata.file_name).all()
    current_kp_status_dict = {}
    for next_current_status in current_kp_status:
        assert next_current_status not in current_kp_status_dict
        current_kp_status_dict[next_current_status.file_name] = next_current_status

    completed_fileroot_dict = {}
    for next_complete_meta in [m for m in current_kp_status_dict.values() if m.ingest_status == config.kp_ingest_status_complete]:
        root, _, _ = get_file_root_plus_extension_with_version_and_revision(next_complete_meta.file_name)
        completed_fileroot_dict[root] = (next_complete_meta.version, next_complete_meta.revision)

    root_versions_revisions = get_kp_files_status(root_dir)

    for file_status_root in root_versions_revisions:  # For on disk roots
        for fullname_version_revision in root_versions_revisions[file_status_root]:  # For on disk root version/revisions
            _, fullname = os.path.split(fullname_version_revision.fully_qualified_name)
            fully_qualified_name = fullname_version_revision.fully_qualified_name
            # do we have a current DB status
            if fullname in current_kp_status_dict:
                current_ingest_status = current_kp_status_dict[fullname].ingest_status
                if current_ingest_status == config.kp_ingest_status_complete:
                    results.complete.append((fully_qualified_name, current_kp_status_dict[fullname]))
                elif current_ingest_status == config.kp_ingest_status_deprecated:
                    results.deprecated.append((fully_qualified_name, current_kp_status_dict[fullname]))
                elif current_ingest_status == config.kp_ingest_status_started:
                    results.started.append((fully_qualified_name, current_kp_status_dict[fullname]))
                elif current_ingest_status == config.kp_ingest_status_updated:
                    results.updated.append((fully_qualified_name, current_kp_status_dict[fullname]))
                elif current_ingest_status == config.kp_ingest_status_failed:
                    continue
                else:
                    raise Exception("The KP metadata ingest status of %s isn't recognized" % current_ingest_status)
            else:  # No current status
                # does a current root status exist?
                if file_status_root in completed_fileroot_dict:  # If someone in this root is complete
                    complete_version_revision = completed_fileroot_dict[file_status_root]  # The completed version/revision
                    check_version_revision = (fullname_version_revision.version, fullname_version_revision.revision)
                    if check_version_revision > complete_version_revision:
                        results.updated.append((fully_qualified_name, None))
                    elif check_version_revision < complete_version_revision:
                        results.deprecated.append((fully_qualified_name, None))
                    else:  # Should never get here....This case should have been caught because a DB status (of COMPLETE) should have been found
                        raise Exception("The KP metadata for file %s wasn't found" % fullname)
                else:
                    results.new.append((fully_qualified_name, None))

    return results


def split_into_latest_and_rest(file_list):
    '''Method used to split the provide list into 2 lists.  The first represents the latest
    version of files in file_list based on the files version/revision.  The second is the
    remaining list of files that aren't the latest
    Returns:
        latest_files [] - List of the latest files
        remaining_files [] - List of the remaining files when the latest have been removed
    '''
    running_latest = {}

    for fn in file_list:
        _, filename = os.path.split(fn)
        root, version, revision = get_file_root_plus_extension_with_version_and_revision(filename)
        if 'crustal' in root:
            continue
        if root not in running_latest:
            running_latest[root] = fn
        else:
            _, existing_filename = os.path.split(running_latest[root])
            _, existing_latest_version, existing_latest_revision = get_file_root_plus_extension_with_version_and_revision(existing_filename)
            if (version, revision) > (existing_latest_version, existing_latest_revision):
                running_latest[root] = fn
    latest_files = list(running_latest.values())
    remaining_files = list(set(file_list) - set(latest_files))

    return latest_files, remaining_files


def ingest_entry_point(new_file):
    ''' This function instantiates the insitu_file_processor() and
        begins the ingest for each file in the list. It also instantiates
        the necessary sqlalchemy objects for each process.

    Arguments:
         new_file - A new file to be ingested.
    '''
    lock = proc_lock
    proc_name = current_process().name
    logger_name = ('maven_in_situ_kp_ingest.{0}.log').format(proc_name)
    local_logger = logging.getLogger(logger_name)
    with create_logger(logger_name) as local_logger:
        try:
            local_logger.info("Ingesting in-situ KP file %s", new_file)
            with insitu_file_processor(new_file, db_session, engine, lock, local_logger) as f:
                start_time = time_utilities.utc_now()
                f.ingest_data()
                local_logger.info("Ingesting in-situ KP file %s took %s", new_file, time_utilities.utc_now() - start_time)
        except Exception as e:
            local_logger.exception("Exception during ingest of %s", new_file)
            err_msg = ('In-situ ingest failed for {0} because of {1}').format(new_file, repr(e))
            # TODO - Do we need Job ID?
            status.add_exception_status(component_id=MAVEN_SDC_COMPONENT.KP_INGESTER,
                                        event_id=MAVEN_SDC_EVENTS.FAIL,
                                        summary=err_msg)
            # Update status to ERROR
            return new_file, -1
        else:
            # Update status to COMPLETE
            return new_file, 2


def initialize_lock(lock):
    ''' Initializes the process lock

        Arguments:
            lock - A multiprocess.Lock() object
    '''
    global proc_lock
    proc_lock = lock


def ingest_in_situ_kp_data(root_dir):
    '''Walks the tree inserting the in-situ key parameter data
    into the database.
    Argument :
        root_dir - The root direction location to find insitu kp files (recursively)
    '''
    # Split the files into new updated current old started
    current_kp_file_status = get_insitu_kp_files_status(root_dir)

    # Process new files (ingest)
    latest_new_files, deprecated_new_files = split_into_latest_and_rest(
        [f[0] for f in current_kp_file_status.new]
    )

    # A subprocess from the pool is given a task and the callback function
    # is called with the return result
    for fp in latest_new_files:
       ingest_entry_point(fp)

    logger.info("ingested files beneath %s", root_dir)

    # Process deprecated files
    for fn in deprecated_new_files:
        add_kp_files_metadata_entry(fn, config.kp_ingest_status_deprecated, db_session)
    for fn, existing_metadata in current_kp_file_status.deprecated:
        if existing_metadata is None:
            add_kp_files_metadata_entry(fn, config.kp_ingest_status_deprecated, db_session)

    # Process updated files
    for fn, existing_metadata in current_kp_file_status.updated:
        if existing_metadata is None:
            add_kp_files_metadata_entry(fn, config.kp_ingest_status_updated, db_session)
