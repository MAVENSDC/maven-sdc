'''
Created on Sep 21, 2015

@author: bstaley
'''
import logging
from maven_database import db_session
from maven_database.models import PdsArchiveRecord

GENERATION_SUCCESS = 'SUCCESS'
GENERATION_FAILURE = 'FAILURE'

logger = logging.getLogger('maven.make_pds_bundles.results.log')


def record_results(generation_time,
                   start_time,
                   end_time,
                   command_line,
                   configuration,
                   dry_run,
                   result_directory,
                   bundle_file_name,
                   manifest_file_name,
                   checksum_file_name,
                   result_version,
                   generation_result,
                   pds_status=None,
                   notes=None):
    '''Method used to record the results of a PDS archive generation'''
    try:
        results = PdsArchiveRecord(generation_time=generation_time,
                                   start_time=start_time,
                                   end_time=end_time,
                                   command_line=command_line,
                                   configuration=configuration,
                                   dry_run=dry_run,
                                   result_directory=result_directory,
                                   bundle_file_name=bundle_file_name,
                                   manifest_file_name=manifest_file_name,
                                   checksum_file_name=checksum_file_name,
                                   result_version=result_version,
                                   generation_result=generation_result,
                                   pds_status=pds_status,
                                   notes=notes)
        db_session.add(results)
        db_session.commit()
    except Exception:
        logger.exception('Unable to store PDS Archive results: %s', results)


def update_result_notes(for_bundle_file_name,
                        notes,
                        append=False):
    '''Method used to update note results of a prior PDS generation
    Arguments:
        for_bundle_file_name - The key to be used to find the PDS archive run record
        note - The updated notes to replace or append
        append - If true, append notes to existing notes,
                 If false, replace existing notes with provided notes
    Returns:
        None if no updates were made, the new entry otherwise
    '''
    existing_entry = PdsArchiveRecord.query.filter(PdsArchiveRecord.bundle_file_name == for_bundle_file_name).all()

    if existing_entry is None or len(existing_entry) == 0:
        logger.warning('Unable to find PDS archive record for bundle name %s', for_bundle_file_name)
        return None
    if len(existing_entry) > 1:
        logger.warning('%s entries found for bundle name %s.  No updates performed', len(existing_entry), for_bundle_file_name)
        return None

    existing_entry[0].notes = notes if not append else '\n'.join([existing_entry[0].notes, notes])
    db_session.commit()

    return existing_entry


def update_result_status(for_bundle_file_name,
                         status):
    '''Method used to update status results of a prior PDS generation
    Arguments:
        for_bundle_file_name - The key to be used to find the PDS archive run record
        status (string) - The new PDS status to record
    Returns:
        None if no updates were made, the new entry otherwise
    '''
    existing_entry = PdsArchiveRecord.query.filter(PdsArchiveRecord.bundle_file_name == for_bundle_file_name).all()

    if existing_entry is None or len(existing_entry) == 0:
        logger.warning('Unable to find PDS archive record for bundle name %s', for_bundle_file_name)
        return None
    if len(existing_entry) > 1:
        logger.warning('%s entries found for bundle name %s.  No updates performed', len(existing_entry), for_bundle_file_name)
        return None

    existing_entry[0].pds_status = status
    db_session.commit()
    return existing_entry
