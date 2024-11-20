# pylint: disable=E1101
'''
Updated on June 5, 2015

@author:mdorey
@author:tbussell

A module that holds the utilities needed by the MAVEN data file indexer.
'''
import os
import logging
from maven_utilities import anc_config, maven_config, constants, file_pattern
from . import utilities

env = os.environ.get(constants.python_env, 'production_update')

console_logger = logging.getLogger('maven.maven_data_file_indexer.maven_data_file_indexer.log')


def upsert_fs_metadata(fs_data, handle_exception=None):
    l0_metadata = []
    sci_metadata = []
    anc_metadata = []
    failed_upserts = []
    for _next in fs_data:
        bn = os.path.basename(_next.path_name)

        if file_pattern.matches_on_group([maven_config.euv_regex,
                                          maven_config.euv_flare_regex,
                                          maven_config.euv_flare_catalog_regex,
                                          maven_config.science_regex,
                                          maven_config.kp_regex,
                                          maven_config.label_regex,
                                          maven_config.sep_anc_regex,
                                          maven_config.euv_l2b_regex,
                                          maven_config.euv_l4_regex],
                                         bn):

            sci_metadata.append(utilities.get_metadata_for_science_file(_next.path_name))
        elif file_pattern.matches_on_group([maven_config.l0_regex],
                                           bn):
            l0_metadata.append(utilities.get_metadata_for_l0_file(_next.path_name))
        elif file_pattern.matches_on_group([maven_config.metadata_index_regex],
                                           bn):
            sci_metadata.append(utilities.get_metadata_for_metadata_file(_next.path_name))
        elif file_pattern.matches_on_group([maven_config.ql_regex],
                                           bn):
            sci_metadata.append(utilities.get_metadata_for_ql_file(_next.path_name))
        elif file_pattern.matches(anc_config.ancillary_regex_list,
                                  bn):
            anc_metadata.append(utilities.get_metadata_for_ancillary_file(_next.path_name))
        else:
            console_logger.warning("The provided file name [%s] does not match any regular expression and won't be upserted!", bn)

    failed_upserts.extend(utilities.upsert_l0_file_metadata(l0_metadata, handle_exception))
    failed_upserts.extend(utilities.upsert_science_file_metadata(sci_metadata, handle_exception))
    failed_upserts.extend(utilities.upsert_ancillary_file_metadata(anc_metadata, handle_exception))

    return failed_upserts


def delete_fs_metadata(fs_data):
    for _next in fs_data:
        bn = os.path.basename(_next.path_name)
        if file_pattern.matches_on_group([maven_config.science_regex,
                                          maven_config.kp_regex,
                                          maven_config.label_regex,
                                          maven_config.sep_anc_regex,
                                          maven_config.euv_l2b_regex,
                                          maven_config.l0_regex,
                                          maven_config.metadata_index_regex,
                                          maven_config.ql_regex,
                                          maven_config.euv_regex,
                                          maven_config.euv_flare_regex,
                                          maven_config.euv_flare_catalog_regex,
                                          maven_config.euv_l4_regex],
                                         bn):
            utilities.delete_science_file_metadata_from_filename(bn)
        elif file_pattern.matches(anc_config.ancillary_regex_list,
                                  bn):
            utilities.delete_ancillary_file_metadata_from_filename(bn)
        else:
            console_logger.warning("The provided file name [%s] does not match any regular expression and won't be removed from the database!", bn)


def run_full_index(root_directories,
                   dry_run=False,
                   handle_exception=None):
    '''Inserts and removes metadata about level 0 and science files into the database.

    Argument
         root_directories - List of directories at the base of the hierarchy that holds the files.
         dry_run - Specifies if the update should run in debug/dry run mode.
    '''
    from . import audit_utilities

    meta_add, meta_delete, meta_update = [], [], []
    failed_upserts = []

    for directory in root_directories:
        db_meta = audit_utilities.get_metadata_from_db(directory)
        disk_meta = audit_utilities.get_metadata_from_disk(directory)
        add, delete, update = audit_utilities.get_metadata_diffs(db_meta, disk_meta)
        meta_add.extend(add)
        meta_delete.extend(delete)
        meta_update.extend(update)

    if not dry_run:
        failed_upserts.extend(upsert_fs_metadata(meta_add, handle_exception))
        failed_upserts.extend(upsert_fs_metadata(meta_update, handle_exception))
        delete_fs_metadata(meta_delete)
    else:
        for metadata in meta_add:
            msg = 'add:{0}'.format(metadata.path_name)
            console_logger.debug(msg)
        for metadata in meta_delete:
            msg = 'delete:{0}'.format(metadata.path_name)
            console_logger.debug(msg)
        for metadata in meta_update:
            msg = 'update:{0}'.format(metadata.path_name)
            console_logger.debug(msg)

    return failed_upserts
