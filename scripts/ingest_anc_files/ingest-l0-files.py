#! /usr/bin/env python
#
# Mike Dorey  2012-11-12

'''A script that moves the MAVEN level 0 files deposited by the POC into /maven/poc
into the instrument directories /maven/data/sci/<instrument>/l0.
'''
import argparse
import sys
from ingest_l0_files import utilities
from maven_status import job, MAVEN_SDC_COMPONENT


def parse_arguments(arguments):
    '''Parses arguments from main()'''
    parser = argparse.ArgumentParser()
    parser.add_argument('src_dir',
                        help='''This is the source directory to find l0 files''')
    parser.add_argument('dest_dir',
                        help='''This is the destination for the moved l0 files''')
    return parser.parse_args(arguments)


def main(arguments):
    args = parse_arguments(arguments[1:])
    utilities.move_files(args.src_dir, args.dest_dir)

if __name__ == "__main__":
    with job.StatusCronJob(MAVEN_SDC_COMPONENT.L0_INGESTER) as job:
        job.run(proc=main,
                proc_args={"arguments": sys.argv})
