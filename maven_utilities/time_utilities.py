'''
Created on Jan 27, 2015

@author: bstaley
'''

import datetime
import pytz

YYYYMMDDTHH_MM_SS_format = '%Y-%m-%dT%H:%M:%S'


def utc_now():
    '''Returns the current UTC datetime with a UTC timezone.'''
    dt = datetime.datetime.utcnow()
    dt = to_utc_tz(dt)
    return dt


def to_utc_tz(dt):
    '''Return a datetime properly converted to the UTC timezone.'''
    if str(dt.tzinfo) == 'UTC':
        return dt

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    dt = dt.astimezone(pytz.UTC)
    return dt


def make_utc_aware(datetime_str, datetime_fmt):
    ''' Creates a UTC aware dt from the provided datetime string and format '''
    ret_val = datetime.datetime.strptime(datetime_str, datetime_fmt)
    return pytz.utc.localize(ret_val)


def total_seconds(td):
    '''Provides the same function as the total_seconds method of timedelta,
       for running under python 2.6.
    '''
    return float((td.microseconds +
                  (td.seconds + td.days * 24 * 3600) * 1.0e6)) / 1.0e6
