#!/maven/mavenpro/venvs/maven_production_py3/bin/python3.6
'''
Created on Dec 03, 2015

@author: bstaley
'''

import os
import sys
import argparse

from maven_public import utilities as maven_public_utils
from maven_utilities import maven_log


def parse_arguments(arguments):
    '''Method used to parse the arguments provided via sys.argv and call the correct maven_public script'''

    parser = argparse.ArgumentParser(description='''Script used to manage MAVEN public web sites
    ''')
    parser.add_argument('root_dir',
                        help='The root directory to be used for the archive output',
                        default='/')
    parser.add_argument('-s', '--symbolic-links',
                        action='store_true',
                        help='If True, site will be built with symbolic links back to production, if False, the production files will be copied to public.')
    parser.add_argument('-d', '--dry-run',
                        action='store_true',
                        help='If True, only a list of files to release will be printed out at /maven/data. No files will be released.')
    return parser.parse_args(arguments)


if __name__ == '__main__':
    os.umask(0o022)
    args = parse_arguments(sys.argv[1:])

    assert os.path.isdir(args.root_dir), '%s is not a directory!' % args.root_dir

    maven_log.config_logging()

    maven_public_utils.build_site(root_dir=args.root_dir,
                                  sym_link=args.symbolic_links,
                                  dry_run=args.dry_run)
