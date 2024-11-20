'''
Created on Jul 15, 2015

This module is responsible for providing the various regex and
helper methods to deal with SDC files names.

@author: bstaley
'''
import re
import logging
from collections import OrderedDict

logger = logging.getLogger('maven.maven_utilities.file_pattern.log')

# General group definitions
general_basename_group = 'base'
general_root_group = 'root'
general_file_root_group = 'file_root'
general_year_group = 'year'
general_month_group = 'month'
general_day_group = 'day'
general_doy_group = 'dayofyear'
general_hh_group = 'hh'
general_mm_group = 'mm'

general_hhmmss_group = 'hhmmss'
general_yyyymmdd_group = 'yyyymmdd'
general_yymmdd_group = 'yymmdd'
general_yymmdd_group_end = 'yymmdd_end'
general_version_revision_group = 'version_revision'
general_version_group = 'version'
general_revision_group = 'revision'
general_extension_group = 'extension'
general_gz_extension_group = 'gz_extension'
general_instrument_group = 'instrument'
general_level_group = 'level'
general_description_group = 'description'
general_mission_group = 'mission'
general_flare_class = 'flare_class'

not_empty_group_regex = re.compile(r'.+')

# Transforms


def thhmmss_extractor(thhmmss):
    '''
    Method used to extract hh mm ss from a thhmmss string
    '''
    if thhmmss is not None and len(thhmmss) == 7:
        hhmmss = int(thhmmss[1:])
        hour, mmss = divmod(hhmmss, 10000)
        minute, second = divmod(mmss, 100)
        return hour, minute, second
    return None, None, None


def remove_underscore_extractor(field_to_remove_underscore):
    return field_to_remove_underscore.replace('_', '')


def safe_int(field):
    if field is not None:
        return int(field)
    return None


def zero_len_to_none(field):
    '''
    Method used to extract zero length matches to None
    '''
    if isinstance(field, str) and len(field) == 0:
        return None
    return field


time_transforms = {general_year_group: int,
                   general_month_group: int,
                   general_day_group: int,
                   general_hhmmss_group: thhmmss_extractor
                   }

ver_rev_transforms = {general_version_group: safe_int,
                      general_revision_group: safe_int
                      }


def matches_on_group(regex_list, string_to_parse, group_regexes=None):
    '''
    Method used to determine the first regular expression (in regex_list) that matches the provided string_to_parse
    Arguments:
        regex_list - A list of compiled regular expressions to be applied in order
        string_to_parse - The string to check against the provided regular expressions
        group_regexes - A list of tuples that contain a list of groups to check as well as the compiled regular expression to check against.
            All provided group_regexs must pass (and'd).  This is used to further refine the regular expression search.
    Returns:
        The first match that satisfies a regular expression found in regex_list as well as ALL group_regexes (if they are provided)
    '''
    for regex in regex_list:
        m = regex.match(string_to_parse)
        if m is not None:
            if group_regexes is not None:
                group_matched = True
                for group, grp_regex in group_regexes:
                    group_val = m.group(group)
                    if group_val is None or not grp_regex.match(m.group(group)):
                        group_matched = False
                        break
                if group_matched:
                    return m
            else:  # matched on regex and there were no groups to match
                return m
    return None


def matches(regex_list, string_to_parse):
    '''
    Method that checks string_to_parse against all regular expressions in regex_list
    and returns True once a match is found, False otherwise
    Arguments:
        regex_list - A list of compiled regular expressions to be applied in order
        string_to_parse - The string to check against the provided regular expressions
    Returns:
        True once a match is found, False otherwise
    '''
    for regex in regex_list:
        m = regex.match(string_to_parse)
        if m is not None:
            return True
    return False


def extract_parts(regex_list, string_to_parse, parts, transforms=None, group_regexes=None, handle_missing_parts=False):
    '''
    Method used to extract the groups provided in order or None if no provided regular expressions match
    Arguments:
        regex_list - A list of compiled regular expressions to be applied in order
        string_to_parse - The string to check against the provided regular expressions
        parts - A list of groups to be extracted
        transforms - A dictionary of group->function (that takes 1 string) to transform the string into some other type
        group_regexes - A list of tuples that contain the group to check as well as the compiled regular expression to check against.
            This is used to further refine the regular expression search.
        handle_missing_parts - If True, missing parts won't raise an exception and the result set will contain None
                              for the missing part.  If False, an IndexError is raised
        Returns:
            A tuple of the groups that were requested in parts or None if there were no matches for the provided regex_list
    '''

    m = matches_on_group(regex_list, string_to_parse, group_regexes)
    if m is None:
        return None

    ret_val = OrderedDict()
    for part in parts:
        try:
            if transforms is not None and part in transforms:
                ret_val[part] = transforms[part](m.group(part))
            else:
                ret_val[part] = m.group(part)
        except IndexError as ie:
            if handle_missing_parts:
                ret_val[part] = None
            else:
                logger.warning('Group %s does not exist!', part)
                raise ie

    return ret_val


def analyze_group_pattern(string_to_parse, pattern, results):
    '''Debug method used to recursively traverse the provided regex to attempt to make a match.
    Useful for debugging regular expressions
    '''
    regex = re.compile(pattern)
    m = regex.match(string_to_parse)
    if m is not None:
        results.append((string_to_parse, pattern, True, m.groupdict()))
        return True

    close_paren_loc = -1
    for i in range(len(pattern) - 1, 1, -1):
        cur_char = pattern[i]
        next_char = pattern[i - 1]
        if cur_char == ')' and next_char != '\\':  # found closing group
            close_paren_loc = i
        elif cur_char == '(' and next_char != '\\':  # found open group
            if close_paren_loc == -1:  # Yuck...
                raise Exception('Malformed regex {}'.format(pattern))
            # Found a group!  remove group and recurse
            new_pattern = pattern[0:i] + '.*' + pattern[close_paren_loc + 1:]
            # Remove wildcard repeats
            new_pattern = re.sub('(\.\*)+', '.*', new_pattern)
            new_pattern = re.sub('\*+', '*', new_pattern)
            results.append((string_to_parse, pattern, False, {}))
            return analyze_group_pattern(string_to_parse, new_pattern, results)
    results.append((string_to_parse, pattern, False, {}))
    return False
