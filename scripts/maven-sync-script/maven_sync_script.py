#! /usr/bin/env python
#
# This is a script that syncs a local directory tree
# to the files that exist at the MAVEN Science Data
# Center.
#
# Mike Dorey  2013-12-16
import argparse
import os
import sys
import urllib
import re
import json
from zipfile import ZipFile
from io import StringIO
from maven_utilities import mail, maven_log, file_pattern


def get_list_of_local_science_files(root_directory):
    '''Returns list of the MAVEN science files that are in the directories
    below the specified root directory on the local disk.
    '''
    local_science_files = []
    for path, dirs, files in os.walk(root_directory):
        for fn in files:
            if is_maven_science_file(fn):
                local_science_files.append(fn)
    return local_science_files


def is_maven_science_file(filename):
    '''Returns True if the specified filename is a MAVEN science file.'''
    return file_pattern.science_pattern.match(filename)


def get_sdc_science_file_info():
    '''Returns a list of dictionaries {'file_name', 'file_size'}.'''
    url = 'http://sdc-web1.pdmz.lasp.colorado.edu/maven/sdc/service/files/api/v1/file_info/science'
    r = urllib.request.urlopen(url)
    status_code = r.getcode()
    assert status_code == 200, '%s failed: %d' % (url, status_code)
    d = json.loads(r.read())
    return d


def get_missing_files(local_root_directory):
    '''Returns (list of files that are at the SDC but are missing on the local disk, total size of download).'''
    local_filename_set = set(get_list_of_local_science_files(local_root_directory))
    sdc_file_info = get_sdc_science_file_info()
    sdc_filename_set = set([d['file_name'] for d in sdc_file_info['files']])
    missing_filenames = sdc_filename_set - local_filename_set
    total_size = 0
    if len(missing_filenames) > 0:
        sdc_dict = {}
        for d in sdc_file_info['files']:
            sdc_dict[d['file_name']] = d['file_size']
        total_size = 0
        for fn in missing_filenames:
            total_size += sdc_dict[fn]
    return (list(missing_filenames), total_size)


def sync_science_files(local_root_directory):
    '''Syncs the files in the local directory to those at the MAVEN Science Data Center.'''
    assert os.path.isdir(local_root_directory), 'specified local directory is not a directory'
    missing_filenames, total_size = get_missing_files(local_root_directory)
    if len(missing_filenames) == 0:
        print('The local directories hold the same files as the MAVEN SDC.')
    else:
        resp = raw_input("There are %d files with a total size of %s to download from the SDC. Download these files? [Y/n] (default is no): " % (len(missing_filenames), formatted_size(total_size)))
        if resp.startswith('Y'):
            while len(missing_filenames) > 0:
                pull_missing_files_from_sdc(local_root_directory, missing_filenames)

                # Try again. The web service may have truncated the zip file.
                missing_filenames, total_size = get_missing_files(local_root_directory)
                if len(missing_filenames) > 0:
                    print('Still working... There are still %d files and %s to fetch from the SDC.' % (len(missing_filenames), formatted_size(total_size)))


def pull_missing_files_from_sdc(local_root_directory, missing_filenames):
    '''Pulls the missing files from the MAVEN SDC and puts them in the proper directory beneath the
    specified local root directory.
    '''
    url = 'http://sdc-web1.pdmz.lasp.colorado.edu/maven/sdc/service/files/api/v1/download_zip/science'
    data = urllib.urlencode({'files': ','.join(missing_filenames)})
    response = urllib.request.urlopen(url, data=data)
    status_code = response.getcode()
    assert status_code == 200, "Download FAILED with status code of %d" % status_code
    content = response.read()
    f = StringIO(content)
    with ZipFile(f) as zf:
        extract_to_dir = get_extract_to_directory(local_root_directory)
        zf.extractall(path=extract_to_dir)


def formatted_size(size):
    '''Returns a string representation of the size in bytes.'''
    if size < 1000000:
        return '%d bytes' % size
    elif size < 1000000000:
        return '%0.2f megabytes' % (size / 1.0e6)
    else:
        return '%0.2f gigabytes' % (size / 1.0e9)


def get_extract_to_directory(root_dir):
    '''Returns the directory into which the MAVEN zip file
    should be extracted. This is the shortest path to a directory that
    holds maven/data/sci.

    If there is no subdirectory that ends with maven/data/sci the root
    directory is returned.
    '''
    assert os.path.isdir(root_dir), 'specified root is not a directory'
    mds_dirs = get_maven_data_sci_directories(root_dir)
    if len(mds_dirs) < 1:
        return root_dir  # did not find a subdirectory that ends with /maven/data/sci so just return root
    else:
        return get_maven_data_sci_root_directory(mds_dirs)


def get_maven_data_sci_root_directory(mds_dirs):
    '''Returns the shortest path to a directory that holds maven/data/sci.'''
    shortest = mds_dirs[0]
    for d in mds_dirs[1:]:
        if len(d) < len(shortest):
            shortest = d
    assert shortest.endswith('/maven/data/sci'), 'Did not find /maven/data/sci in shortest directory name'
    return shortest.rstrip('/maven/data/sci')


def get_maven_data_sci_directories(root_dir):
    '''Returns a list of the directories that end with /maven/data/sci.'''
    tree = []
    for path, dirs, files in os.walk(root_dir):
        for d in dirs:
            subdir = os.path.join(path, d)
            if os.path.isdir(subdir):
                tree.append(subdir)
    mds_dirs = [directory for directory in tree if directory.endswith('/maven/data/sci')]
    return mds_dirs


def parse_arguments(arguments):
    '''Parses arguments from main()'''
    parser = argparse.ArgumentParser()
    parser.add_argument('sync_dir',
                        help='''Directory to sync''')
    return parser.parse_args(arguments)


def main(arguments):
    args = parse_arguments(arguments[1:])
    try:
        sync_science_files(args.sync_dir)
    except Exception as e:
        err_msg = 'Unable to sync science files under %s' % args.sync_dir
        mail.send_exception_email(subject='MAVEN Sync Script CRITICAL ERROR',
                                  message=err_msg,
                                  sender_username='mavenpro@lasp.colorado.edu')

if __name__ == "__main__":
    maven_log.config_logging()
    main(sys.argv)
