#! /usr/bin/env python
#
# A script to check for new MAVEN files in any directory
# or set of directories, and create a chunk of Javascript giving
# links to the filenames,
# which can then be loaded in the website.
#
# Alex DeWolfe 2014-05-22
# Adapted from new_files.py
#

import os
from . import config


def list_files(src_dir):
    '''Makes a list of new files'''

    filenames = sorted(next(os.walk(src_dir))[2])
    return [os.path.join(src_dir, fn) for fn in filenames]


def write_js(files, dest_file):
    '''Writes list as a Javascript array for use by the SDC website

    Arguments
        files - list of filenames
    '''
    filenamestring = '["{0}"]'.format('","'.join(files))
    with open(dest_file, 'w') as fd:
        fd.write(filenamestring)


def quicklook(inst_pkg):
    '''Main:

    Arguments
        inst_pkg - instrument package, either PFP or IUV.
    See quicklook_config.py for more info.
    '''

    if inst_pkg == 'pfp':
        inst = config.pf_source
    if inst_pkg == 'iuv':
        inst = config.iuv_source

    filenamelist = []
    inst = inst.split(',')
    for item in inst:
        
        #Team website
        path = os.path.join(config.source_root, item, 'ql')
        filenamelist = list_files(path)
        dest_file = os.path.join(config.destination_dir, item + '.js')
        write_js(filenamelist, dest_file)
        
        #Public website
        public_path = os.path.join(config.public_source_root, item, 'ql')
        public_filenamelist = list_files(public_path)
        public_dest_file = os.path.join(config.public_destination_dir, item + '.js')
        write_js(public_filenamelist, public_dest_file)
