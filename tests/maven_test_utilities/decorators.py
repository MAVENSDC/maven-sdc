'''
Created on Jun 8, 2015

@author: bstaley
         tbussell

Useful decorators for testing.
'''
from functools import wraps
import time


def print_execution_time(func):
    @wraps(func)
    def test_print_time_wrapper(*args, **kwargs):
        now = time.time()
        result = func(*args, **kwargs)
        print (func.__name__ + ' took ' + str(time.time() - now) + 's')
        return result
    return test_print_time_wrapper
