#! /usr/bin/env python
#
# Usage:
#   maven-dropbox-mgr.py <itf dropbox root directory>
#
# Kim Kokkonen 2014-09-03
import sys

'''A script to move the MAVEN files from the dropbox to
their home in shared storage.'''
import argparse
import logging
from maven_dropbox_mgr import utilities
from maven_status import job, MAVEN_SDC_COMPONENT


def parse_arguments(arguments):
    '''Parses arguments from main()'''
    parser = argparse.ArgumentParser()
    parser.add_argument('src_dirs',
                        help='''Source itf dropbox root directories''',
                        nargs='+')
    return parser.parse_args(arguments)


def run_dropbox(src_dirs):
    for itf_dropbox_dir in args.src_dirs:
        utilities.move_files_in_directory_tree(itf_dropbox_dir)

if __name__ == "__main__":
    args = parse_arguments(sys.argv[1:])
    with job.StatusCronJob(MAVEN_SDC_COMPONENT.DROPBOX) as job:
        job.run(proc=run_dropbox,
                proc_args={'src_dirs': args.src_dirs})
