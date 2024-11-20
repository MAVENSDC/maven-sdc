'''
Unit tests for the maven_orbit package
Created on Nov 2, 2015

@author: bstaley
'''
import os
import pytz
import unittest
from tempfile import mkstemp
from shutil import rmtree, move
from datetime import datetime, timedelta

from maven_utilities import time_utilities
from tests.maven_test_utilities import file_system, db_utils, decorators
from maven_orbit import config as maven_orbit_config
from maven_orbit import maven_orbit
from maven_database.models import MavenOrbit


class TestMavenOrbit(unittest.TestCase):

    def setUp(self):
        self.root_directory = file_system.get_temp_root_dir()
        self.orbit_period_in_days = 4.25 / 24.0

    def tearDown(self):
        rmtree(self.root_directory)
        self.assertFalse(os.path.isdir(self.root_directory))
        db_utils.delete_data(MavenOrbit)

    def test_synch_orbit(self):
        '''Test the ability to generate/re-generate SDC orbit numbers
        in the SDC database'''
        orb_file_1 = create_naif_orbit_file(self.root_directory,
                                            datetime(2014, 1, 1, tzinfo=pytz.UTC),
                                            datetime(2014, 3, 1, tzinfo=pytz.UTC),
                                            1,
                                            self.orbit_period_in_days,
                                            True)
        orb_file_2 = create_naif_orbit_file(self.root_directory,
                                            datetime(2014, 3, 1, tzinfo=pytz.UTC),
                                            datetime(2014, 5, 1, tzinfo=pytz.UTC),
                                            count_orbits(os.path.join(self.root_directory, orb_file_1)) + 1,
                                            self.orbit_period_in_days)

        expected_orbits = count_orbits(os.path.join(self.root_directory, orb_file_1)) + count_orbits(os.path.join(self.root_directory, orb_file_2))

        maven_orbit.synchronize_orbit_data(self.root_directory)
        self.assertEqual(expected_orbits, MavenOrbit.query.count())

        maven_orbit.synchronize_orbit_data(self.root_directory)
        self.assertEqual(expected_orbits, MavenOrbit.query.count())

        # test perigee time
        orbit_num = 1
        periapse = maven_orbit.get_orbit_perigee_time(orbit_num)
        orbit_full = MavenOrbit.query.filter(MavenOrbit.orbit_number == orbit_num).first()
        query = MavenOrbit.query.count()
        self.assertEqual(query, expected_orbits)
        self.assertEqual(periapse, orbit_full.orbit_periapse)
        
        # remove a file
        os.remove(os.path.join(self.root_directory, orb_file_2))
        expected_orbits = count_orbits(os.path.join(self.root_directory, orb_file_1))

        maven_orbit.synchronize_orbit_data(self.root_directory)
        self.assertEqual(expected_orbits, MavenOrbit.query.count())

        # remove all files
        os.remove(os.path.join(self.root_directory, orb_file_1))
        expected_orbits = 0

        maven_orbit.synchronize_orbit_data(self.root_directory)
        self.assertEqual(expected_orbits, MavenOrbit.query.count())

    def test_orbit_perigee_time_empty(self):
        '''Test the ability for the maven_orbit package to detect
        empty time slots 
        '''
        orbit_num = 1
        periapse = maven_orbit.get_orbit_perigee_time(orbit_num)
        query = MavenOrbit.query.count()
        self.assertEqual(query, 0)
        self.assertEqual(periapse, None)

        
    def test_modify_orbit(self):
        '''Test the ability for the maven_orbit package to detect
        and handle changes in the NAIF orbit files'''
        orb_file_1 = create_naif_orbit_file(self.root_directory,
                                            datetime(2014, 1, 1, tzinfo=pytz.UTC),
                                            datetime(2014, 3, 1, tzinfo=pytz.UTC),
                                            1,
                                            self.orbit_period_in_days)
        perigee_before_update = time_utilities.utc_now()
        update_naif_orbit(from_file=os.path.join(self.root_directory, orb_file_1),
                          orbit_num=1,
                          new_perigee_time_dt=perigee_before_update)
        maven_orbit.synchronize_orbit_data(self.root_directory)

        orbit_1 = MavenOrbit.query.filter(MavenOrbit.orbit_number == 1).first()
        self.assertEqual(1, orbit_1.orbit_number)

        expected_perigee = datetime(2009, 8, 3, tzinfo=pytz.UTC)
        update_naif_orbit(from_file=os.path.join(self.root_directory, orb_file_1),
                          orbit_num=1,
                          new_perigee_time_dt=expected_perigee)
        maven_orbit.synchronize_orbit_data(self.root_directory)

        orbit_1 = MavenOrbit.query.filter(MavenOrbit.orbit_number == 1).first()
        self.assertEqual(1, orbit_1.orbit_number)
        self.assertEqual(expected_perigee, orbit_1.orbit_periapse)

    @unittest.skip('skipping')
    def test_full_orbit_synch_run(self):
        '''Test to determine worst case synch times'''
        maven_life_in_years = 10
        test_start_dt = datetime(2014, 1, 1, tzinfo=pytz.UTC)
        days_in_a_file = 90
        total_days = timedelta(days=356 * maven_life_in_years + maven_life_in_years % 4)
        test_end_dt = test_start_dt + total_days
        orbit_count = 0
        orbit_files = []

        while test_start_dt < test_end_dt:
            test_remaining = test_end_dt - test_start_dt
            file_duration = min([test_remaining, timedelta(days=days_in_a_file)])
            end_dt = test_start_dt + file_duration
            next_orb_file = create_naif_orbit_file(self.root_directory,
                                                   test_start_dt,
                                                   end_dt,
                                                   orbit_count + 1,
                                                   self.orbit_period_in_days)
            test_start_dt = end_dt
            orbit_count += count_orbits(os.path.join(self.root_directory, next_orb_file))
            orbit_files.append(next_orb_file)

        @decorators.print_execution_time
        def run_sync():
            maven_orbit.synchronize_orbit_data(self.root_directory)

        # load all
        run_sync()
        # synch on no change
        run_sync()
        # synch on removed data
        os.remove(os.path.join(self.root_directory, orbit_files[0]))
        run_sync()
        # synch on added data
        last_orbit_number = get_last_orbit_number()
        next_orb_file = create_naif_orbit_file(self.root_directory,
                                               test_end_dt,
                                               test_end_dt + timedelta(days=days_in_a_file),
                                               last_orbit_number + 1,
                                               self.orbit_period_in_days)

        run_sync()


naif_orbit_entry_template = '{0:>5}  {1:>20}  {2:>20}  {3:>20}  {4:>7}  {5:>7}  {6:>7}  {7:>7}  {8:>10}  {9:>12}  \n'


def get_last_orbit_number():
    orbit = MavenOrbit.query.order_by(MavenOrbit.orbit_number.desc()).first()
    if orbit:
        return orbit.orbit_number
    return None


def count_orbits(from_file):
    '''Helper method used to count the valid number of orbit
    definitions in the NAIF orbit file'''
    count = 0
    with open(from_file) as naif_orbit_file:
        for next_line in naif_orbit_file:
            m = maven_orbit_config.naif_orbit_entry_regex.match(next_line)
            if m:
                count += 1
    return count


def update_naif_orbit(from_file,
                      orbit_num,
                      new_perigee_time_dt=None,
                      new_sclock_time=None,
                      new_apogee_time_dt=None,
                      new_sol_lon=None,
                      new_sol_lat=None,
                      new_sc_lon=None,
                      new_sc_lat=None,
                      new_alt=None,
                      new_sol_dist=None):
    '''Helper method used to update a row in a valid NAIF orbit file'''

    temp_fh, temp_path = mkstemp()

    with open(temp_path, 'w') as temp_file:
        with open(from_file, 'r') as naif_orbit_file:
            for next_line in naif_orbit_file:
                m = maven_orbit_config.naif_orbit_entry_regex.match(next_line)
                if m:
                    if int(m.group(maven_orbit_config.orbit_number)) == orbit_num:
                        updated_orbit_str = naif_orbit_entry_template.format(
                            orbit_num,
                            new_perigee_time_dt.strftime(maven_orbit_config.naif_time_format) if new_perigee_time_dt else m.group(maven_orbit_config.peri_utc),
                            new_sclock_time if new_sclock_time else m.group(maven_orbit_config.peri_sclock),
                            new_apogee_time_dt.strftime(maven_orbit_config.naif_time_format) if new_apogee_time_dt else m.group(maven_orbit_config.apogee_utc),
                            new_sol_lon if new_sol_lon else m.group(maven_orbit_config.sol_lon),
                            new_sol_lat if new_sol_lat else m.group(maven_orbit_config.sol_lat),
                            new_sc_lon if new_sc_lon else m.group(maven_orbit_config.sc_lon),
                            new_sc_lat if new_sc_lat else m.group(maven_orbit_config.sc_lat),
                            new_alt if new_alt else m.group(maven_orbit_config.alt),
                            new_sol_dist if new_sol_dist else m.group(maven_orbit_config.sol_dist)
                        )
                        temp_file.write(updated_orbit_str)
                        continue
                    temp_file.write(next_line)
    os.close(temp_fh)
    os.remove(from_file)
    move(temp_path, from_file)


def create_naif_orbit_file(directory, from_dt, to_dt, starting_orbit_number=1, days_between_orbits=.93, is_running=False):
    '''Helper method used to generate a valid NAIF orbit file'''
    spacecraft_start_of_life = datetime(2014, 1, 1, tzinfo=pytz.UTC)
    file_name = 'maven_orb_rec.orb' if is_running else 'maven_orb_rec_' + from_dt.strftime('%Y%m%d') + '_' + to_dt.strftime('%Y%m%d') + '_v1.orb'
    with open(os.path.join(directory, file_name), 'w') as f:
        # write header
        f.write(naif_orbit_entry_template.format(' No.',
                                                 ' Event UTC PERI',
                                                 ' Event SCLK PERI',
                                                 ' OP-Event UTC APO',
                                                 ' SolLon',
                                                 ' SolLat',
                                                 ' SC Lon',
                                                 ' SC Lat',
                                                 ' Alt',
                                                 ' Sol Dist'
                                                 ))

        f.write(naif_orbit_entry_template.format('=' * 5,
                                                 '=' * 20,
                                                 '=' * 20,
                                                 '=' * 20,
                                                 '=' * 7,
                                                 '=' * 7,
                                                 '=' * 7,
                                                 '=' * 7,
                                                 '=' * 10,
                                                 '=' * 12
                                                 ))
        next_date = from_dt
        while next_date <= to_dt:
            apogee_dt = next_date
            perigee_dt = next_date + timedelta(days=days_between_orbits / 2.0)
            perigee_sclock = (perigee_dt - spacecraft_start_of_life).total_seconds()
            f.write(naif_orbit_entry_template.format(starting_orbit_number,
                                                     perigee_dt.strftime(maven_orbit_config.naif_time_format),
                                                     perigee_sclock,
                                                     apogee_dt.strftime(maven_orbit_config.naif_time_format),
                                                     42.0,
                                                     42.0,
                                                     42.0,
                                                     42.0,
                                                     42000.0,
                                                     42000000.0
                                                     ))
            next_date = next_date + timedelta(days=days_between_orbits)
            starting_orbit_number += 1

        return file_name
