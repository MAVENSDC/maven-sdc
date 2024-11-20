#! /usr/bin/env python
#
# A script that puts metadata about MAVEN level 0, 1, 2, 3, KP,
# and quicklook science files into a database.
#
# If the metadata about a file already exist in the database new metadata
# are not added.
#
# Mike Dorey  2013-05-01
# Tyler Bussell 2015-06-05
import argparse
import sys
import time
from sqlalchemy.exc import DataError

from maven_data_file_indexer import maven_file_indexer
from maven_status import job, MAVEN_SDC_COMPONENT, MAVEN_SDC_EVENTS, status


def parse_arguments(arguments):
    parser = argparse.ArgumentParser()
    parser.add_argument('root_directories',
                        help='''You must specify at least one root directory.
                        Multiple directories should be separated by a space.''',
                        nargs='+')
    parser.add_argument('-d', '--dryrun',
                        help='''True will result in a dry run that does
                        not attempt to insert data into the database.
                        Defaults to normal run''',
                        type=bool,
                        default=False)
    parser.add_argument('-u', '--unique-id',
                        help='''Allow the indexer to run with the provided unique-id.
                        Only 1 instance of an indexer per unique-id can be running at any
                        given time''',
                        default="")
    return parser.parse_args(arguments)


def main(arguments):
    args = parse_arguments(arguments[1:])
    if args.dryrun:
        start = time.time()
        maven_file_indexer.run_full_index(args.root_directories, args.dryrun)
        print ('Indexing SciFi and L0 took:', time.time() - start)
    else:
        failed_upserts = []
        failed_upserts.extend(maven_file_indexer.run_full_index(args.root_directories, args.dryrun, handle_exception=DataError))

        if len(failed_upserts) > 0:
            status.add_status(component_id=MAVEN_SDC_COMPONENT.FULL_INDEXER,
                              event_id=MAVEN_SDC_EVENTS.FAIL,
                              summary='Some files were not indexed',
                              description='The following files were not indexed:%s' % '\n\t'.join(['Error - %s.  File - %s' % (err, meta) for err, meta in failed_upserts]))

if __name__ == '__main__':
    with job.StatusCronJob(MAVEN_SDC_COMPONENT.FULL_INDEXER) as job:
        job.run(proc=main,
                proc_args={'arguments': sys.argv})
