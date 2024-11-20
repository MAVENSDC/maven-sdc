'''
Created on Jun 1, 2015

@author: tbussell
'''
import unittest
import datetime
import os
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
import pytz


class TimeUtilitiesUnitTests(unittest.TestCase):

    def setUp(self):
        self.dt_no_tz = datetime.datetime.utcnow()
        self.dt_utc_tz = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)

    def test_make_utc_aware(self):
        test_time1 = datetime.datetime.utcnow()
        test_time2 = test_time1.replace(microsecond=0, tzinfo=pytz.UTC)
        test_format1 = time_utilities.YYYYMMDDTHH_MM_SS_format
        time_string1 = test_time1.strftime(test_format1)
        result = time_utilities.make_utc_aware(time_string1, test_format1)

        self.assertEqual(None, test_time1.tzinfo)
        self.assertEqual(pytz.UTC, result.tzinfo)
        self.assertEqual(test_time2, result)

    def test_utc_now(self):
        dt_now = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
        util_dt_now = time_utilities.utc_now()
        delta = util_dt_now - dt_now
        self.assertLess(int(delta.total_seconds()), 2)

    def test_to_utc_tz(self):

        dt_no_tz = time_utilities.to_utc_tz(self.dt_no_tz)
        self.assertEqual(str(dt_no_tz.tzinfo), 'UTC')

        dt_utc_tz = time_utilities.to_utc_tz(self.dt_utc_tz)
        self.assertEqual(str(dt_utc_tz.tzinfo), 'UTC')
        self.assertEqual(dt_utc_tz, dt_utc_tz)