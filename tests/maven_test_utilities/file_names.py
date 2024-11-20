'''
Created on Mar 28, 2016

@author: bstaley
'''

from maven_utilities import time_utilities, maven_config


def generate_science_file_name(instrument='iuv',
                               level='l1a',
                               description='test-description',
                               file_dt=None,
                               version=1,
                               revision=1,
                               extension='fits',
                               gz_extension=None):

    file_dt = file_dt if file_dt else time_utilities.utc_now()

    result = 'mvn_{0}_{1}_{2}_{3}_v{4:02d}_r{5:02d}.{6}'.format(instrument,
                                                                level,
                                                                description,
                                                                file_dt.strftime('%Y%m%dT%H%M%S'),
                                                                version,
                                                                revision,
                                                                extension
                                                                )

    if gz_extension:
        result = '{0}.{1}'.format(result,
                                  gz_extension)

    assert maven_config.science_regex.match(result) is not None

    return result
