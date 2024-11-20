'''
Created on Nov 11, 2015

@author: bstaley

Unit tests for MAVEN Orbit database access
'''

import os
import unittest
import datetime
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'
import maven_database
from maven_database.models import MavenOrbit
from tests.maven_test_utilities import db_utils


class MavenOrbitDbAccessTestCase(unittest.TestCase):
    '''Tests access of the database table that holds the maven orbits.'''

    def setUp(self):
        pass

    def tearDown(self):
        db_utils.delete_data(MavenOrbit)

    def test_for_smoke(self):
        count = MavenOrbit.query.count()
        self.assertTrue(count is not None)

    def test_insert(self):
        '''Test that string formats are handled'''
        self.assertEqual(0, MavenOrbit.query.count())

        orbit = MavenOrbit(orbit_number=42,
                           orbit_periapse=time_utilities.utc_now(),
                           orbit_apoapse=time_utilities.utc_now(),
                           synched_at=time_utilities.utc_now(),
                           synched_source='Unit Test')

        maven_database.db_session.add(orbit)
        maven_database.db_session.commit()
        after_count = MavenOrbit.query.count()
        self.assertEqual(after_count, 1)
        o2 = MavenOrbit.query.first()
        self.assertEqual(o2.orbit_number, orbit.orbit_number)
        self.assertEqual(o2.orbit_periapse, orbit.orbit_periapse)
        self.assertEqual(o2.orbit_apoapse, orbit.orbit_apoapse)
        self.assertEqual(o2.synched_at, orbit.synched_at)
        self.assertEqual(o2.synched_source, orbit.synched_source)
        string_rep = MavenOrbit(o2.orbit_number,
                                o2.orbit_periapse,
                                o2.orbit_apoapse,
                                o2.synched_at,
                                o2.synched_source)
        self.assertEqual(str(string_rep), 'MavenOrbit \n\tOrbit number %s\n\tOrbit perigee %s\n\tOrbit apogee %s\n\tSynced At %s\n\tSynch Source %s' % (o2.orbit_number,
                                                                                                                                   o2.orbit_periapse,
                                                                                                                                   o2.orbit_apoapse,
                                                                                                                                   o2.synched_at,
                                                                                                                                   o2.synched_source))

    def test_equality(self):
        assert MavenOrbit.query.count() == 0, 'there were orbits before test_insert ran'
        orbit = MavenOrbit(orbit_number=42,
                           orbit_periapse=time_utilities.utc_now(),
                           orbit_apoapse=time_utilities.utc_now(),
                           synched_at=time_utilities.utc_now(),
                           synched_source='Unit Test')

        maven_database.db_session.add(orbit)
        maven_database.db_session.commit()

        test_orbit = MavenOrbit(orbit_number=orbit.orbit_number,
                                orbit_periapse=orbit.orbit_periapse,
                                orbit_apoapse=orbit.orbit_apoapse,
                                synched_at=time_utilities.utc_now(),
                                synched_source=orbit.synched_source)

        self.assertEqual(orbit, test_orbit)

        # test inequality
        test_orbit = MavenOrbit(orbit_number=orbit.orbit_number,
                                orbit_periapse=orbit.orbit_periapse + +datetime.timedelta(hours=1),
                                orbit_apoapse=orbit.orbit_apoapse,
                                synched_at=time_utilities.utc_now(),
                                synched_source=orbit.synched_source)

        self.assertNotEqual(orbit, test_orbit)

        test_orbit = MavenOrbit(orbit_number=orbit.orbit_number,
                                orbit_periapse=orbit.orbit_periapse,
                                orbit_apoapse=orbit.orbit_apoapse + datetime.timedelta(hours=1),
                                synched_at=time_utilities.utc_now(),
                                synched_source=orbit.synched_source)

        self.assertNotEqual(orbit, test_orbit)

        test_orbit = MavenOrbit(orbit_number=orbit.orbit_number,
                                orbit_periapse=orbit.orbit_periapse,
                                orbit_apoapse=orbit.orbit_apoapse,
                                synched_at=time_utilities.utc_now(),
                                synched_source=orbit.synched_source + '_foo')

        self.assertNotEqual(orbit, test_orbit)
