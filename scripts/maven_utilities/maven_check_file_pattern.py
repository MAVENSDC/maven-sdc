#!/maven/mavenpro/venvs/maven_production_py3/bin/python3.6
'''
Stand-alone script used to analyze a filename pattern against a set of regular expressions
to determine which expression match the pattern and where particular expression fail to
match the pattern.

Created on Jul 20, 2015

@author: bstaley
'''
import argparse
import inspect
import sys
import re
from maven_utilities import file_pattern, maven_config, anc_config

compiled_regex_type = type(re.compile('^42'))


def parse_arguments(arguments):
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output-format',
                        help='Output format %%p pattern %%r regex %%N regex_name %%a analysis %%R result %%n newline %%t tab',
                        default='%R %t%p against %N')
    parser.add_argument('-p', '--patterns',
                        help='''Patterns to test''',
                        nargs='*')
    parser.add_argument('-r', '--regexs',
                        help='''Regexs to use (if they exist)''',
                        nargs='+')
    parser.add_argument('-s', '--show-results',
                        help='''Show matches,non-matches, both''',
                        choices=['matches', 'non-matches', 'both'],
                        default='both')
    return parser.parse_args(arguments)


def regex_predicate(obj):
    return isinstance(obj, compiled_regex_type)


def dump_analyze(analyze_results, pretty_print=False):
    results = []
    for next_result in analyze_results:
        if pretty_print:
            results.append('Result=>{} Pattern=>{} Regex=>{} Groups=>{}'.format(next_result[2], next_result[0], next_result[1], next_result[3]))
        else:
            results.append('{},{},{},{}'.format(next_result[2], next_result[0], next_result[1], next_result[3]))
    return '\t'.join(results)


def generate_output_string_format(requested_output_format):
    return requested_output_format\
        .replace('{', '{{')\
        .replace('}', '}}')\
        .replace('%p', '{pattern}')\
        .replace('%r', '{regex}')\
        .replace('%N', '{name}')\
        .replace('%a', '{analysis}')\
        .replace('%R', '{result}')\
        .replace('%g', '{groups}')\
        .replace('%n', '\n')\
        .replace('%t', '\t')


def process_next_pattern(test_pattern,
                         requested_output_format,
                         test_regexes,
                         show_match=False,
                         show_miss=False):
    output_string = generate_output_string_format(requested_output_format)

    for next_regex_inspection in test_regexes:

        analyze_results = []
        matched = file_pattern.analyze_group_pattern(test_pattern, next_regex_inspection[1].pattern, analyze_results) and len(analyze_results) == 1

        if (matched and show_match) or (not matched and show_miss):
            print(output_string.format(pattern=test_pattern,
                                       regex=next_regex_inspection[1].pattern,
                                       name=next_regex_inspection[0],
                                       analysis=dump_analyze(analyze_results, True),
                                       result=matched,
                                       groups='\n'.join([str(x[3]) for x in analyze_results if len(x[3]) > 0]),
                                       ))

if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])
    file_regexes = inspect.getmembers(file_pattern, regex_predicate)
    file_regexes.extend(inspect.getmembers(maven_config, regex_predicate))
    file_regexes.extend(inspect.getmembers(anc_config, regex_predicate))

    if args.regexs is not None:
        requested_regexes = []

        for name, regex in file_regexes:
            if name in args.regexs:
                requested_regexes.append((name, regex))
        file_regexes = requested_regexes

    if args.patterns:
        for test_pattern in args.patterns:
            process_next_pattern(test_pattern,
                                 args.output_format,
                                 file_regexes,
                                 show_match=(args.show_results == 'both' or args.show_results == 'matches'),
                                 show_miss=(args.show_results == 'both' or args.show_results == 'non-matches')
                                 )
            print ('\n')

    else:  # stdin
        for test_pattern in sys.stdin.readlines():
            process_next_pattern(test_pattern.rstrip(),
                                 args.output_format,
                                 file_regexes,
                                 show_match=(args.show_results == 'both' or args.show_results == 'matches'),
                                 show_miss=(args.show_results == 'both' or args.show_results == 'non-matches')
                                 )
            print ('\n')
