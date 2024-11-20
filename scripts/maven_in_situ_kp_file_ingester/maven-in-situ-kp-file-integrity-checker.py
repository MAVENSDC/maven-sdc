#! /usr/bin/env python
'''
A script that checks for common errors in the in-situ KP ingest process.

Created on Aug 19, 2015

@author: bstaley
'''
import sys
import argparse

from maven_in_situ_kp_file_ingester import in_situ_kp_file_processor


def parse_arguments(arguments):
    '''Parses arguments from main()'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file',
                        help='''file to check''',
                        required=True)
    return parser.parse_args(arguments)


def run_check(file_to_check):
    '''Method used to run the in_situ_kp_file_processor utilities to verify
    the integrity of the provided file
    Arguments:
      file_to_check - the file to check
    '''

    format_tuple = None
    try:
        #        in_situ_kp_file_processor.build_format_pattern(file_to_check)
        with open(file_to_check) as f:
            for line in f:
                if in_situ_kp_file_processor.identify_in_situ_column_line(line):
                    format_tuple = in_situ_kp_file_processor.process_in_situ_column_line(line)
                    break

    except Exception as e:
        print e
        print "%s doesn't contain a column format line!" % file_to_check
        return 1
    with open(file_to_check) as f:
        comment_lines = {}
        format_lines = {}
        data_lines = {}
        unknown_lines = {}

        failed_format_lines = {}
        failed_data_lines = {}

        for line_count, line in enumerate(f, 1):

            if in_situ_kp_file_processor.identify_in_situ_format_line(line, format_pattern=format_tuple.value):
                format_lines[line_count] = line
            elif in_situ_kp_file_processor.identify_in_situ_data_line(line):
                data_lines[line_count] = line
            elif in_situ_kp_file_processor.identify_in_situ_comment_line(line):
                comment_lines[line_count] = line
            else:
                unknown_lines[line_count] = line

        for key, value in format_lines.items():
            try:
                result = in_situ_kp_file_processor.process_in_situ_format_line(value, format_pattern=format_tuple.value)
                if result is None:
                    failed_format_lines[key] = value
            except Exception as e:
                print e
                failed_format_lines[key] = value

        for key, value in data_lines.items():
            try:
                result = in_situ_kp_file_processor.process_in_situ_data_line(value)
                if result is None:
                    failed_data_lines[key] = value
            except:
                failed_data_lines[key] = value

    # Print report
    print ('IN-SITU KP Integrity Report for ', file_to_check)
    print ('================================================================================')
    print ('Identified format lines', len(format_lines))
    print ('Identified data lines', len(data_lines))
    print ('Identified comment lines', len(comment_lines))
    print ('Identified Unknown lines', len(unknown_lines))
    print ('================================================================================')
    print ('Failed format lines', len(failed_format_lines))
    print ('Failed format line #s %s' % ','.join([str(i) for i in failed_format_lines.keys()]))
    print ('Failed data lines', len(failed_data_lines))
    print ('Failed data line #s %s' % ','.join([str(i) for i in failed_data_lines.keys()]))
    print ('================================================================================')


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])
    print 'Processing', args.file
    run_check(args.file)
