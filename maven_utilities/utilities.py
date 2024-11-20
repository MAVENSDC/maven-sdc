'''
Created on May 18, 2015

@author: tbussell
'''
import os
import time
import hashlib
import datetime
from maven_utilities import file_pattern, maven_config, time_utilities


def get_mtime(file_to_check):
    '''Method used to return the modfied time of the file provided
    Arguments:
        file_to_check : The fully qualified path for the file to check
    Returns:
        A TZ aware datetime if the file exists or None if the file doesn't exist'''
    if os.path.exists(file_to_check):
        return time_utilities.to_utc_tz(datetime.datetime.fromtimestamp(os.path.getmtime(file_to_check)))
    return None


def files_are_same(file1, file2):
    '''Returns True if the files are the same.'''
    
    if not os.path.isfile(file1) or not os.path.isfile(file2):
        return False
    if os.stat(file1).st_size == os.stat(file2).st_size:
        with open(file1, 'rb') as f:
            file1_contents = f.read()
        with open(file2, 'rb') as f:
            file2_contents = f.read()
        h1 = hashlib.md5(file1_contents).hexdigest()
        h2 = hashlib.md5(file2_contents).hexdigest()
        return h1 == h2

    return False

    
def is_compressed_format(filename):
    '''Method used to determine if the provided file is in gzip format
    Returns:
        True if the provided file is gzip, False otherwise
    '''
    with open(filename, 'rb') as f:
        header = f.read(2)
        return header == b'\x1f\x8b'


def file_is_old_enough(path, age_limit):
    '''Returns True if the file is old enough. It takes two arguments:

        path : fully qualified file name including path
        age_limit : the threshold in seconds which is used to to determine
                    if a file is old enough.

        '''
    now = time.time()
    mtime = os.stat(path).st_mtime
    return now - mtime > age_limit


def listdir_files(directory, recursive=False, fully_qualified_name=False):
    '''Returns a list of all the files that are in a directory.
    Each element is a complete pathname based on the given directory name.
    If recursive is True, all subdirectories are walked and files in
    the subdirectories are also included in the list.
    '''
    all_names = os.listdir(directory)

    pathnames = []
    if recursive:
        for path, _, files in os.walk(directory):
            for f in files:
                pathnames.append(os.path.join(directory, path, f) if fully_qualified_name else f)
    else:
        all_names = os.listdir(directory)
        pathnames = [os.path.join(directory, n) if fully_qualified_name else n for n in all_names if os.path.isfile(os.path.join(directory, n))]
    return pathnames


def get_file_root_plus_extension(file_name, default=None):
    '''Returns the portion of the file name up to but not including the version
    plus the extension.
    '''
    root, _, _ = get_file_root_plus_extension_with_version_and_revision(file_name)

    if root is None:
        return default
    return root


def get_file_root_plus_extension_with_version_and_revision(file_name):
    '''Returns the portion of the file name up to but not including the version
    plus the extension along with the version and revision as a tuple
    Returns:
        (root,version,revision)
    '''
    m = file_pattern.extract_parts([maven_config.root_verrev_ext_regex],
                                   file_name,
                                   [file_pattern.general_root_group,
                                    file_pattern.general_extension_group,
                                    file_pattern.general_version_group,
                                    file_pattern.general_revision_group])

    if m is None:
        return (None, None, None)
    root, ext, version, revision = m.values()
    return '.'.join([root, ext]), int(version) if version else None, int (revision) if revision else None


def get_absolute_version(version=None,
                         revision=None):
    '''Return an absolute version based on version/revision
    '''
    # pylint: disable=R1716
    assert revision is None or (revision >= 0 and revision < 1000)
    return (version * 1000 if version is not None else 0) + (revision if revision else 0)
