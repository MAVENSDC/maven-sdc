'''
Created on Jun 14, 2016

@author: bstaley
'''


def generateDatesInRange(start_dt, end_dt, step):
    '''Method used to generate a set of times that are >= start_dt and < end_dt that increment by step
    Arguments:
        start_dt : Start datetime
        end_dt : End datetime
        step : timedelta between yield values
    Yields:
        datetimes that are >= start_dt and < end_dt that increment by step
    '''
    curr_dt = start_dt
    while curr_dt < end_dt:
        yield curr_dt
        curr_dt += step
