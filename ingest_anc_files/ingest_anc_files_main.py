'''
Created on Jun 11, 2015

@author: tbussell

Move the MAVEN ancillary data files from the directory where
they arrive to their proper ancillary directories.
'''
import argparse
from ingest_anc_files import config
from ingest_anc_files import utilities


def parse_arguments(arguments):
    '''Parses arguments from main()'''
    parser = argparse.ArgumentParser()
    parser.add_argument('src_dir',
                        help='''Move the MAVEN ancillary data files from the
                                directory where they arrive to their proper
                                ancillary directories''')
    return parser.parse_args(arguments)


def main(arguments):
    args = parse_arguments(arguments[1:])
    utilities.ingest_anc_files(args.src_dir,
                               config.root_destination_directory,
                               config.dupe_dir_name)
