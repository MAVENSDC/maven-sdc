'''
Functions used to provide the argument parsing for the make_pds_bundles scripts.
Created on Jun 11, 2015

@author: bstaley
'''

import os
import argparse

from . import make_pds_bundles, config

def parse_arguments(arguments):
    all_instruments = list(config.instrument_config)
    all_instruments.append(config.all_key)

    parser = argparse.ArgumentParser(description='''Script used to generate PDS archive packages for the various MAVEN instruments
    Examples:
      #Generate the IUV PDS Archive for Jan/Feb of 2015
      make-pds-bundles.py 2015-01-01 2015-03-01 / -i iuv

      #Generate the IUV SWE and SWI instrument Archives for Jan/Feb of 2015
      make-pds-bundles.py 2015-01-01 2015-03-01 / -i iuv swe swi


      #Generate All the instrument Archives for Jan/Feb of 2015
      make-pds-bundles.py 2015-01-01 2015-03-01 / -i all

      #Generate the instrument manifests for Jan/Feb of 2015
      make-pds-bundles.py 2015-01-01 2015-03-01 / -i all -d

      #Dump the instrument configurations for all instruments
      make-pds-bundles.py 2015-01-01 2015-03-01 / -i all -dcl
    ''')
    parser.add_argument('date_range',
                        nargs=2,
                        help='The date range [start,end) which is inclusive for start and exclusive for end')
    parser.add_argument('root_dir',
                        help='The root directory to be used for the archive output',
                        default='/')
    parser.add_argument('-i', '--instruments',
                        choices=all_instruments,
                        nargs='+',
                        help='The list of instrument for which to generate PDS archives')
    parser.add_argument('-d', '--dry-run',
                        action='store_true',
                        help='Will produce the transfer manifest but will not generate the time consuming PDS archive')
    parser.add_argument('-c', '--print-instrument-config',
                        action='store_true',
                        help='Will send the instrument config to stdout for the requested instruments')
    parser.add_argument('-l', '--print-instrument-lid',
                        action='store_true',
                        help='Will send the instrument dictionary to stdout for the requested instruments')
    parser.add_argument('-r', '--report',
                        action='store_true',
                        help='Generate a report as opposed to generating the actual archive')
    parser.add_argument('-n', '--notes',
                        default='',
                        help='Add a note to the results record')
    parser.add_argument('-o', '--override',
                        default=None,
                        help='Override the instrument_config variable located in config.py with another file')
    parser.add_argument('-s', '--skip-missing-labels',
                        action='store_true',
                        help='Do not bundle files that do not have corresponding label files')
    args = parser.parse_args(arguments)
    return args


def main(date_range, root_dir, instruments, dry_run, print_instrument_config=False, print_instrument_lid=False, report=False, notes=None, override=None, skip_missing_labels=False):
    '''Method used to call the correct make_pds_bundles script'''
    assert os.path.isdir(root_dir), '%s is not a directory!' % root_dir
    if override:
        assert os.path.isfile(override), '%s is not a file!' % override
    
    if print_instrument_config:
        make_pds_bundles.print_instrument_config(instruments)
    if print_instrument_lid:
        make_pds_bundles.print_instrument_dictionary(instruments)
    if report:
        make_pds_bundles.run_report(date_range[0], date_range[1], instruments)
    else:
        make_pds_bundles.run_archive(date_range[0], date_range[1], instruments, root_dir, dry_run, notes, override, skip_missing_labels)

