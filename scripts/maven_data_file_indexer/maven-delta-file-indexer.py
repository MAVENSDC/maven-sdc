#! /usr/bin/env python
#
# Bryan Staley 2016-03-14
import argparse
import sys
from maven_data_file_indexer import maven_delta_indexer
from maven_status import job, MAVEN_SDC_COMPONENT


def parse_arguments(arguments):
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--root-directories',
                        help='''You must specify one root directory.''',
                        default='/maven/data/sci /maven/data/anc',
                        nargs='+',
                        required=True)
    parser.add_argument('-u', '--unique-id',
                        help='''Allow the indexer to run with the provided unique-id.
                        Only 1 instance of an indexer per unique-id can be running at any
                        given time''',
                        default="")
    return parser.parse_args(arguments)


def main(arguments):

    args = parse_arguments(arguments[1:])
    root_indexer = maven_delta_indexer.get_root_indexer(args.root_directories)
    root_indexer.process_events()

if __name__ == "__main__":
    with job.StatusCronJob(MAVEN_SDC_COMPONENT.DELTA_INDEXER) as job:
        job.run(proc=main,
                proc_args={'arguments': sys.argv})
