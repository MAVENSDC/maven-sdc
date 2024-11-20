#! /usr/bin/env python
'''
An application to remove the old version/revision files from the MAVEN SDC.

Created on Oct 20, 2016

@author: bstaley
'''

import argparse
import sys

from maven_file_cleaner import utilities
from maven_status import job, MAVEN_SDC_COMPONENT


def main(arguments):
    parser = argparse.ArgumentParser()
    parser.add_argument('directories',
                        help='''Clean obsolete files in each given directory.''',
                        nargs='+')
    parser.add_argument('-R', '--recursive',
                        help='''Search recursively down from all given directories''',
                        action='store_true')
    parser.add_argument('-v', '--num-versions',
                        help='''Number of latest versions to keep''')
    parser.add_argument('-r', '--num-revisions',
                        help='''Number of latest revisions to keep''')
    parser.add_argument('-d', '--dry-run',
                        help='''Set to just print the files that would be deleted''',
                        action='store_true')

    args = parser.parse_args(arguments)

    for directory_to_clean in args.directories:
        utilities.clean_directory(directory=directory_to_clean,
                                  recursive=args.recursive,
                                  num_versions_to_keep=int(args.num_versions) if args.num_versions else args.num_versions,
                                  num_revisions_to_keep=int(args.num_revisions) if args.num_revisions else args.num_revisions,
                                  dry_run=args.dry_run)


if __name__ == '__main__':
    with job.StatusJob(MAVEN_SDC_COMPONENT.DISK_CLEANER) as job:
        job.run(proc=main,
                proc_args={'arguments': sys.argv[1:]},
                propagate_exceptions=True)
