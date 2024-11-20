'''
Created on Oct 20, 2016

@author: bstaley
'''
import os
import logging
from collections import OrderedDict
from maven_utilities import utilities as util_utilities

logger = logging.getLogger('maven.maven_file_cleaner.utilities.log')


def build_version_revision_data(directory, recursive=True):
    '''Method used to build a {root:[((v,r),fully_qualified_file_name)]} structure 
    Arguments:
        directory : Top level directory to start searching for files
        recursive : If True, recurse into directory children.  False only produce files from directory
    Returns:
        {root:{ver:{rev:pn}}} structure where results are sorted by version, revision descending
        per file root
    '''
    results = {}
    version_data = {}
    # Get list of pns
    for pn in util_utilities.listdir_files(directory=directory, recursive=recursive, fully_qualified_name=True):
        root, ver, rev = util_utilities.get_file_root_plus_extension_with_version_and_revision(os.path.basename(pn))
        if root:
            version_data.setdefault(root, []).append(((ver, rev), pn))
        else:
            logger.warning('No version information for %s.  File will not be considered for removal', pn)
    # sort em
    for key in version_data:
        version_data[key].sort(reverse=True)
        # Convert to an more usable format
        for _next in version_data[key]:
            ver_rev, pn = _next
            results.setdefault(key, OrderedDict()).setdefault(ver_rev[0], OrderedDict()).setdefault(ver_rev[1],
                                                                                                    pn)
    return results


def get_latest_version_revision_data(v_r_data, num_versions_to_keep=None, num_revisions_to_keep=None):
    '''Method used to split the v_r_data into latest and other
    Arguments:
        v_r_data - The {root:{ver:{rev:pn}}} that contains the raw version/revision data
        num_versions_to_keep - Integer used to determine how many latest version to return as newest
        num_revisions_to_keep - Integer used to determine how many latest revisions to return as newest
    Returns
        A dictionary with root as the key and 2 lists (newest followed by oldest) as the value
        e.g {root:[[newest files],[oldest files]]}   
    '''

    results = {}

    # Traverse the  {root:{ver:{rev:pn}}} data and split into latest and other
    for root in v_r_data:
        newest = []
        oldest = []

        vers_processed = 0
        for ver in v_r_data[root]:
            if num_versions_to_keep is not None and vers_processed >= num_versions_to_keep:
                for rev in v_r_data[root][ver]:
                    oldest.append(v_r_data[root][ver][rev])
            else:
                revs_processed = 0
                for rev in v_r_data[root][ver]:
                    if num_revisions_to_keep is not None and revs_processed >= num_revisions_to_keep:
                        oldest.append(v_r_data[root][ver][rev])
                    else:
                        newest.append(v_r_data[root][ver][rev])
                    revs_processed += 1
            vers_processed += 1
        results[root] = [newest, oldest]
    return results


def clean_directory(directory, recursive=False, num_versions_to_keep=None, num_revisions_to_keep=None, dry_run=False):
    '''Method used to find the oldest files based on version/revision and remove them from the file system
    Arguments:
        directory - The root directory to start the cleansing process
        recursive - Boolean used to determine if the run should traverse into child directories to find files
        num_versions_to_keep - Integer used to determine how many latest version to return as newest
        num_revisions_to_keep - Integer used to determine how many latest revisions to return as newest
        dry_run - True - don't remove any files just report what's to be done.  False remove files from the file system
    '''

    # build version data
    version_data = build_version_revision_data(directory=directory,
                                               recursive=recursive)

    # split into keep/discard
    root_tree = get_latest_version_revision_data(v_r_data=version_data,
                                                 num_versions_to_keep=num_versions_to_keep,
                                                 num_revisions_to_keep=num_revisions_to_keep)

    dump_root_tree(root_tree)

    bytes_freed = 0

    for next_root in root_tree:
        for next_old_file in root_tree[next_root][1]:
            next_bytes_freed = os.path.getsize(next_old_file)
            bytes_freed += next_bytes_freed

            if dry_run:
                logger.info('Would have deleted %s to free %s bytes', next_old_file, next_bytes_freed)
            else:
                logger.info('Removing files %s to free %s bytes', next_old_file, next_bytes_freed)
                os.remove(next_old_file)

    logger.info('You freed %s bytes!', bytes_freed)


def dump_root_tree(root_tree):
    '''Method used to dump a human readable version of the latest/oldest data
    Arguments:
        root_tree - The root tree {root:[[latest],[oldest]]}
    '''
    for next_root in root_tree:
        print (next_root)
        print (u'\t+--NEWEST')
        for next_new_file in root_tree[next_root][0]:
            print (u'\t|\t+--%s' % next_new_file)
        print (u'\t+--OLDEST')
        for next_old_file in root_tree[next_root][1]:
            print (u'\t\t+--%s' % next_old_file)
        print ()
