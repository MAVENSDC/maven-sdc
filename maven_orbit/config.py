'''
Created on Nov 2, 2015

@author: bstaley
'''
import re

naif_time_format = '%Y %b %d %H:%M:%S'
start_YYMMDD = 'startYYMMDD'
end_YYMMDD = 'endYYMMDD'

naif_running_orbit_file_name = 'maven_orb_rec.orb'
naif_orbit_file_pattern = (r'maven_orb_rec'
                           '(?:|_(?P<{0}>[0-9]{{6}})_(?P<{1}>[0-9]{{6}}))'
                           '.*\.orb').format(start_YYMMDD,
                                             end_YYMMDD)

naif_orbit_file_regex = re.compile(naif_orbit_file_pattern)

orbit_number = 'orbit_number'
peri_utc = 'peri_utc'
peri_sclock = 'peri_sclock'
apogee_utc = 'apo_utc'
sol_lon = 'sol_lon'
sol_lat = 'sol_lat'
sc_lon = 'sc_lon'
sc_lat = 'sc_lat'
alt = 'alt'
sol_dist = 'sol_dist'

naif_orbit_entry_pattern = (r'^(?P<{0}>[ 0-9]{{5}})  '
                            '(?P<{1}>.{{20}})  '
                            '(?P<{2}>.{{20}})  '
                            '(?P<{3}>.{{20}})  '
                            '(?P<{4}>.{{7}})  '
                            '(?P<{5}>.{{7}})  '
                            '(?P<{6}>.{{7}})  '
                            '(?P<{7}>.{{7}})  '
                            '(?P<{8}>.{{10}})  '
                            '(?P<{9}>.{{12}})'

                            ).format(orbit_number,
                                     peri_utc,
                                     peri_sclock,
                                     apogee_utc,
                                     sol_lon,
                                     sol_lat,
                                     sc_lon,
                                     sc_lat,
                                     alt,
                                     sol_dist)
naif_orbit_entry_regex = re.compile(naif_orbit_entry_pattern)
