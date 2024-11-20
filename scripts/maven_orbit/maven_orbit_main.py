#! /usr/bin/env python
#
# A script that synchronizes the MAVEN orbit information from NAIF to the SDC database
#
#
# Bryan Staley 2015-11-02

import argparse
import sys

from maven_orbit import maven_orbit
from maven_status import job, MAVEN_SDC_COMPONENT


def parse_arguments(arguments):
    '''Method used to parse the command line arguments provided.
    Arguments:
        arguments - sys.argv less sys.argv[0]
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('root_directory',
                        help='You must specify one root directory.',
                        default='/')

    return parser.parse_args(arguments)

if __name__ == "__main__":
    args = parse_arguments(sys.argv[1:])
    with job.StatusCronJob(MAVEN_SDC_COMPONENT.ORBIT) as job:
        job.run(proc=maven_orbit.synchronize_orbit_data,
                proc_args={'root_directory': args.root_directory})