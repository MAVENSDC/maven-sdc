#! /usr/bin/env python
'''
Main for calling quicklook package.

@author: Kim Kokkonen
'''
import argparse
import sys
from monitoring import quicklook
from maven_utilities import mail, maven_log
from maven_status import job, MAVEN_SDC_COMPONENT


def parse_arguments(arguments):
    '''Parses arguments from main()'''
    parser = argparse.ArgumentParser()
    parser.add_argument('inst_pkg',
                        help='''Instrument package''')
    return parser.parse_args(arguments)

if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])
    with job.StatusCronJob(MAVEN_SDC_COMPONENT.QL_MONITOR) as job:
        job.run(proc=quicklook.quicklook,
                proc_args={'inst_pkg': args.inst_pkg})
