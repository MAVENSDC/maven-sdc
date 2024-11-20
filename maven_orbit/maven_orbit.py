'''
Package used to synchronize the orbit data received from NAIF to the
MAVEN SDC database.

Created on Nov 2, 2015

@author: bstaley
'''
import os
from datetime import datetime
import pytz
import logging

from . import orbit_db_session
from maven_database.models import MavenOrbit
from . import config as maven_orbit_config

logger = logging.getLogger('maven.maven_orbit.maven_orbit.log')


def get_orbit_perigee_time(orbit_number):
    '''Method used to retrieve the MavenOrbit.orbit_periapse time for the provided orbit number
    Returns:
        The periapse time or None if the orbit_number doesn't exist
    '''
    orbit = MavenOrbit.query.filter(MavenOrbit.orbit_number == orbit_number).first()

    if orbit is None:
        return None
    return orbit.orbit_periapse


def generate_orbit_numbers(src_file, update_datetime):
    '''Helper generator used to generate MavenOrbit objects
    based on the data within a NAIF orbit file.
    Arguments:
      src_file : The source file to be used to generate MavenOrbits
      update_datetime : The datetime to be assigned to the newly generated MavenOrbit
    '''
    with open(src_file) as naif_orbit_file:
        for next_line in naif_orbit_file:
            m = maven_orbit_config.naif_orbit_entry_regex.match(next_line)
            if not m:
                logger.info('The NAIF orbit line %s is not a proper orbit entry for regex %s', next_line, maven_orbit_config.naif_orbit_entry_pattern)
                continue
            try:
                yield MavenOrbit(orbit_number=int(m.group(maven_orbit_config.orbit_number)),
                                 orbit_periapse=datetime.strptime(m.group(maven_orbit_config.peri_utc), maven_orbit_config.naif_time_format).replace(tzinfo=pytz.UTC),
                                 orbit_apoapse=datetime.strptime(m.group(maven_orbit_config.apogee_utc), maven_orbit_config.naif_time_format).replace(tzinfo=pytz.UTC),
                                 synched_at=update_datetime,
                                 synched_source=src_file)
            except ValueError as _:
                # Happens in non reconstructed orbit files with new orbit entries
                pass


def synchronize_orbit_data(root_directory):
    '''Method of ensuring the SDC database orbit information is in sync
    with the orbit files provide by NAIF
    NOTE - No sub-directories will be searched.
    Arguments:
      root_directory : The root directory to search for NAIF orbit files.
    '''
    orbits = MavenOrbit.query.all()

    orbits_dict = {}
    for orbit in orbits:
        orbits_dict[orbit.orbit_number] = orbit

    removed_orbit_ids = set(orbits_dict.keys())

    # Prune list
    naif_files = [fn for fn in os.listdir(root_directory) if os.path.isfile(os.path.join(root_directory, fn)) and maven_orbit_config.naif_orbit_file_regex.match(fn)]
    # Sort list ensuring running NAIF file is last
    naif_files.sort(key=lambda x: (not maven_orbit_config.naif_orbit_file_regex.match(x).group(maven_orbit_config.start_YYMMDD), x))

    # Walk over all source
    for fn in naif_files:
        fn = os.path.join(root_directory, fn)
        logger.info('processing NAIF file %s', fn)
        file_mod_time = datetime.utcfromtimestamp(os.path.getctime(fn)).replace(tzinfo=pytz.UTC)
        for next_orbit in generate_orbit_numbers(fn, file_mod_time):
            if next_orbit.orbit_number not in orbits_dict.keys():
                orbit_db_session.add(next_orbit)
            else:
                removed_orbit_ids.remove(next_orbit.orbit_number)
                if not next_orbit == orbits_dict[next_orbit.orbit_number]:
                    # pylint: disable=E1101
                    orbit_db_session.merge(next_orbit)
                    orbits_dict[next_orbit.orbit_number] = next_orbit
        orbit_db_session.commit()

    # Remove orbits that weren't updated
    for orbit_num in removed_orbit_ids:
        logger.info('Orbit no longer exists.  Removing orbit %s', orbit_num)
        # pylint: disable=E1101
        orbit_db_session.delete(orbits_dict[orbit_num])
    orbit_db_session.commit()
