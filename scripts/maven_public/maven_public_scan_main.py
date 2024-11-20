#! /usr/bin/env python
'''
Created on Jan 20, 2016

Script used to detect and/or remove dead symlinks
@author: bstaley
'''

import os
import sys
import argparse

from maven_public import utilities as maven_public_utils


def parse_arguments(arguments):
    '''Method used to parse the arguments provided via sys.argv'''

    parser = argparse.ArgumentParser(description='''Script used to scan the MAVEN public web site and clean up any dead symlinks
    ''')
    parser.add_argument('root_dir',
                        help='The root directory to scan for dead symlinks')
    parser.add_argument('-r', '--remove-links',
                        action='store_true',
                        help='If True, dead symlinks will be removed from the system, if False, dead symlinks will be reported.')
    return parser.parse_args(arguments)


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    assert os.path.isdir(args.root_dir), '%s is not a directory!' % args.root_dir

    if args.delete_links:
        maven_public_utils.clean_site(args.root_dir)
    else:
        for next_link in maven_public_utils.check_site(args.root_dir):
            print next_link
