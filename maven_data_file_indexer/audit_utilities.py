'''
A set of common utilities used for indexing and auditing the science/ancillary 
metadata

Created on Oct 18, 2017

@author: bstaley
'''

import os
import subprocess
from datetime import datetime
from collections import namedtuple
from maven_database.models import ScienceFilesMetadata, AncillaryFilesMetadata
from maven_utilities import file_pattern, maven_config, anc_config, time_utilities

FSMetadata = namedtuple('FSMetadata', ['path_name', 'file_size', 'mod_date'])

# The set of regualr expression to be considered when auditing files on disk
regexs = [maven_config.sep_anc_regex,
          maven_config.euv_l2b_regex,
          maven_config.science_regex,
          maven_config.kp_regex,
          maven_config.label_regex,
          maven_config.metadata_index_regex,
          maven_config.l0_regex,
          maven_config.ql_regex,
          maven_config.euv_regex,
          maven_config.euv_flare_regex,
          maven_config.euv_flare_catalog_regex,
          maven_config.euv_l4_regex]
regexs.extend(anc_config.ancillary_regex_list)


def get_metadata_from_disk(directory):
    '''Use the linux find utility to get all the files under root_directory
    with size and mod_date as quickly as possible. regexes is
    a list of compiled regular expressions used to match against the files on disk.
    Returns a list of FSMetadata tuples, sorted by path_name.
    Arguments:
        directory - The unix file system directory to process
    Returns:
        A sorted set of FSMetadata representing the files under the provided directory
    '''
    fs_metadata = []
    # find for path_name, file_size, and ctime
    args = ['/usr/bin/find', directory, '-type', 'f', '-printf', '%p^%s^%T@\n']
    h = subprocess.Popen(args, bufsize=65536, stdout=subprocess.PIPE).stdout
    for l in h:
        pn, size, ctime = l.decode().split('^')
        f = os.path.basename(pn)
        # keep only files matching one of the regexes
        # TODO Can we get rid of this check?  If something made it to the SDC, it should be index regardless of its pattern
        if file_pattern.matches(regexs, f):
            # convert unix timestamp to UTC-aware datetime
            cdate = time_utilities.to_utc_tz(datetime.utcfromtimestamp(float(ctime[:len(ctime) - 1])))
            fs_metadata.append(FSMetadata(pn, int(size), cdate))

    return sorted(fs_metadata, key=lambda m: m.path_name)


def get_metadata_from_db(directory=None):
    '''
    A method to retrieve the current set of ancillary and science metadata for the provided directory from the database
    Arguments:
        directory - Only retieve metadata for the provided directory.  If directory is None, retrieve all metadata
    Returns:
        A sorted set of FSMetadata
    '''
    fs_metadata = []
    scifi = ScienceFilesMetadata.query.with_entities(
        ScienceFilesMetadata.directory_path,
        ScienceFilesMetadata.file_name,
        ScienceFilesMetadata.mod_date,
        ScienceFilesMetadata.file_size)
    if directory:
        scifi = scifi.filter(
            ScienceFilesMetadata.directory_path.like("{0}%".format(directory))).all()
    ancfi = AncillaryFilesMetadata.query.with_entities(
        AncillaryFilesMetadata.directory_path,
        AncillaryFilesMetadata.file_name,
        AncillaryFilesMetadata.mod_date,
        AncillaryFilesMetadata.file_size)
    if directory:
        ancfi = ancfi.filter(
            AncillaryFilesMetadata.directory_path.like("{0}%".format(directory))).all()

    for _next in scifi:
        mod_date = _next.mod_date or time_utilities.utc_now()
        fs_metadata.append(FSMetadata(os.path.join(_next.directory_path, _next.file_name), _next.file_size, mod_date))
    for _next in ancfi:
        mod_date = _next.mod_date or time_utilities.utc_now()
        fs_metadata.append(FSMetadata(os.path.join(_next.directory_path, _next.file_name), _next.file_size, mod_date))

    return sorted(fs_metadata, key=lambda m: m.path_name)


def get_metadata_diffs(metadata_db, metadata_fs):
    '''Given two lists of FSMetadata tuples from database and filesystem,
    sorted by path_name, return three lists: 
        files that need to be added to the db, 
        files that need to be deleted from the db, and 
        files whose contents changed that must be updated in the db.
    Arguments:
        metadata_db - The list of FSMetadata from the database
        metadata_fs - The list of FSMetadata from disk
    Returns:
        A tuple of (added, deleted, modified) where added represents metadata to be added to the database, 
        deleted represents metadata to be deleted from the database and modified represents metadata to be
        updated in the database)
    '''
    metadata_add = []
    metadata_delete = []
    metadata_update = []

    # walk the two lists independently
    # looking for differences
    i_db = 0
    i_fs = 0
    while i_db < len(metadata_db) or i_fs < len(metadata_fs):
        md_db = metadata_db[i_db] if i_db < len(metadata_db) else None
        md_fs = metadata_fs[i_fs] if i_fs < len(metadata_fs) else None
        if md_db is not None and md_fs is not None:
            # both lists have an element
            if md_db.path_name == md_fs.path_name:
                # existing file, has it changed?
                if (md_db.file_size != md_fs.file_size or
                        # microsecond accuracy isn't possible
                        md_db.mod_date.replace(microsecond=0) != md_fs.mod_date.replace(microsecond=0)):
                    metadata_update.append(md_fs)
                i_db += 1
                i_fs += 1
            elif md_fs.path_name < md_db.path_name:
                # disk file added
                metadata_add.append(md_fs)
                i_fs += 1
            else:
                # disk file deleted
                metadata_delete.append(md_db)
                i_db += 1
        elif md_db is None:
            # no more db elements, disk file added
            metadata_add.append(md_fs)
            i_fs += 1
        else:
            # no more fs elements, disk file deleted
            metadata_delete.append(md_db)
            i_db += 1

    return metadata_add, metadata_delete, metadata_update
