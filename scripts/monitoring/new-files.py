#! /usr/bin/env python
'''
Main for calling new_files package.

@author: Kim Kokkonen
'''
import argparse
import sys
from monitoring import new_files
from maven_status import job, MAVEN_SDC_COMPONENT


def parse_arguments(arguments):
    '''Parses arguments from main()'''
    parser = argparse.ArgumentParser()
    parser.add_argument('paths',
                        help='''A comma separated string of directories ''')
    parser.add_argument('subdirs',
                        help='''A boolean determining whether do
                                subdirs in a single directory ''',
                        type=bool)
    parser.add_argument('dst_file',
                        help='''The destination file path''')
    parser.add_argument('num_files',
                        help='''The maximum number of files displayed.
                                If num_files <= 0 all will be displayed.''',
                        type=int)
    return parser.parse_args(arguments)

if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])
    with job.StatusCronJob(MAVEN_SDC_COMPONENT.NEW_MONITOR) as job:
        job.run(proc=new_files.main,
                proc_args={'paths': args.paths,
                           'subdirs': args.subdirs,
                           'dst_file': args.dst_file,
                           'num_files': args.num_files})
