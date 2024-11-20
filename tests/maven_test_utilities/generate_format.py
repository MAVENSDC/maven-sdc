'''
Created on Jul 30, 2015

@author: cosc3564
'''


def inSituGenerateFormatLine(indent, parameter, instrument, units, column_num, format_, notes):
    ''' Method used to generate 1 FORMAT in-situ KP line
    Returns: A string that represents a FORMAT line of an in-situ kp file
    Indent used for child formats
    '''
    if indent is None:
        indentation = '{:<56}'
        child_indent = ''
    else:
        indentation = '{:<54}'
        child_indent = '  '

    return '# %s%s  %s  %s  %s  %s  %s\n' % (child_indent,
                                             indentation.format(parameter),
                                             '{:<10}'.format(instrument),
                                             '{:<16}'.format(units),
                                             '{:>8}'.format(column_num),
                                             '{:<20}'.format(format_),
                                             '{:<158}'.format(notes)
                                             )


def inSituGenerateDataLine(time, values):
    ''' Method used to generate 1 DATA in-situ KP line
    Returns: A string that represents a DATA line of an in-situ kp file
    '''
    time_str = time.strftime('%Y-%m-%dT%H:%M:%S')
    val1 = '{:>12}'.format(values[0])
    valn = ''
    for val in values[1:]:
        valn += '{:>16}'.format(val)
    return '%s %s%s\n' % (time_str, val1, valn)
