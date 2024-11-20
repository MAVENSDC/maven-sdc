#! /usr/bin/env python
'''
Created on May 18, 2015

Script used to scan the directory tree for .gz files.  If a .gz (gzipped) file is found
the script will unzip and remove the .gz files.  This move is recoreded in the maven_dropbox_mgr_moves
tables of the MAVEN SDC database

@author: bstaley
'''
import sys
import os
import gzip
import shutil
from maven_dropbox_mgr import utilities as dropbox_utils
from maven_utilities import maven_log, utilities

holding_dir = '/tmp/zip_clean/'


def unzip(filename):
    with gzip.GzipFile(filename, 'r') as gzipfile:
        try:
            # Create unzipped file
            tmp_filename = os.path.splitext(filename)[0]

            with open(tmp_filename, 'w') as tmp_file:
                tmp_file.write(gzipfile.read())
                # Remove zipped file from file system
                # os.remove(filename)

                shutil.move(filename, holding_dir)
                dropbox_utils.log_move_in_db(filename, tmp_filename)
        except IOError as e:
            err = 'Unable to unzip file %s due to %s' % (filename, str(e))
            print err
            raise RuntimeError(err)

if __name__ == '__main__':
    maven_log.config_logging()
    if len(sys.argv) is not 2:
        print ('Invalid args: %s. Usage maven_clean_zips.py <root of directory to scan>' % sys.argv)
        exit(-1)

    if not os.path.isdir(holding_dir):
        os.makedirs(holding_dir)

    print ('Scanning ', sys.argv[1])
    for root, dirs, files in os.walk(sys.argv[1]):
        for f in [f for f in files if utilities.is_compressed_format(f)]:
            full_file_name = os.path.join(root, f)
            print ('Unzipping %s' % full_file_name)
            unzip(full_file_name)
