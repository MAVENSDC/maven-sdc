# pylint: disable=E1101
'''
Created on Dec 8, 2014

@author: bstaley

  Methods used to parse the in-situ key parameters data file.  The file format and data sections are
  expected to be formatted correctly (see identify_in_situ... methods regular expressions).
'''
import os
import re
import math
import pytz
from datetime import datetime
from collections import namedtuple
from logging import getLogger
from threading import Thread
from maven_in_situ_kp_file_ingester import config
from maven_database import models
from maven_utilities import file_pattern, maven_config
# from maven_test_utilities.decorators import print_execution_time

''' Named Tuple to hold data associated with derived in-situ parameters. '''
DerivedParameterTuple = namedtuple('DerivedParameterTuple', 'name formula units format')

format_pattern_kwarg = 'format_pattern'

KeywordArgTuple = namedtuple('KeywordArgTuple', 'key,value')

''' The meta-data associated with derived in-situ parameters
    NOTE: Derived parameters are computed in order (top to bottom).  Dependent derived parameters
          need to have their dependencies defined prior in the derived_parameter_formulas list'''
derived_parameter_formulas = [DerivedParameterTuple('mag_mso_magnitude',
                                                    # mso_magnitude = sqrt(msox^2 + msoy^2 + msoz^2)
                                                    'math.sqrt(mag_magnetic_field_mso_x**2 + mag_magnetic_field_mso_y**2 + mag_magnetic_field_mso_z**2 )',
                                                    'nT',
                                                    'E9.2'),
                              DerivedParameterTuple('mag_geo_magnitude',
                                                    # geo_magnitude = sqrt(geox^2 + geoy^2 + geoz^2)
                                                    'math.sqrt(mag_magnetic_field_geo_x**2 + mag_magnetic_field_geo_y**2 + mag_magnetic_field_geo_z**2)',
                                                    'nT',
                                                    'E9.2'),
                              DerivedParameterTuple('mag_mso_clock_angle',
                                                    # mso_clock_angle = atan2(msoy,msoz)
                                                    'math.atan2(mag_magnetic_field_mso_y,mag_magnetic_field_mso_z)',
                                                    'radians',
                                                    'E9.2'),
                              DerivedParameterTuple('mag_geo_clock_angle',
                                                    # geo_clock_angle = atan2(geoy,geoz)
                                                    'math.atan2(mag_magnetic_field_geo_y,mag_magnetic_field_geo_z)',
                                                    'radians',
                                                    'E9.2'),
                              DerivedParameterTuple('mag_mso_cone_angle',
                                                    # mso_cone_angle = acos(fabs(msox)/mso_magnitude)
                                                    'math.acos(math.fabs(mag_magnetic_field_mso_x)/mag_mso_magnitude)',
                                                    'radians',
                                                    'E9.2'),
                              DerivedParameterTuple('mag_geo_cone_angle',
                                                    # geo_cone_angle = acos(fabs(geox)/geo_magnitude)
                                                    'math.acos(math.fabs(mag_magnetic_field_geo_x)/mag_geo_magnitude)',
                                                    'radians',
                                                    'E9.2'),
                              DerivedParameterTuple('mag_geo_radial_component',
                                                    # radial_component = geox*clon*clat+geoy*slon*clat+geoz*slat
                                                    'mag_magnetic_field_geo_x*math.cos(spacecraft_spacecraft_geo_longitude*math.pi/180)*math.cos(spacecraft_spacecraft_geo_latitude*math.pi/180)+mag_magnetic_field_geo_y*math.sin(spacecraft_spacecraft_geo_longitude*math.pi/180)*math.cos(spacecraft_spacecraft_geo_latitude*math.pi/180)+mag_magnetic_field_geo_z*math.sin(spacecraft_spacecraft_geo_latitude*math.pi/180)',
                                                    'radians',
                                                    'E9.2'),
                              DerivedParameterTuple('mag_geo_eastward_component',
                                                    # eastward_component = data.geoy * clon - data.geox * slon
                                                    'mag_magnetic_field_geo_y * math.cos(spacecraft_spacecraft_geo_longitude*math.pi/180) - mag_magnetic_field_geo_x * math.sin(spacecraft_spacecraft_geo_longitude*math.pi/180)',
                                                    'radians',
                                                    'E9.2'),
                              DerivedParameterTuple('mag_geo_northward_component',
                                                    # northward_component = data.geoz * clat - data.geox * clon * slat - data.geoy * slon * slat
                                                    'mag_magnetic_field_geo_z * math.cos(spacecraft_spacecraft_geo_latitude*math.pi/180) - mag_magnetic_field_geo_x * math.cos(spacecraft_spacecraft_geo_longitude*math.pi/180) * math.sin(spacecraft_spacecraft_geo_latitude*math.pi/180) -  mag_magnetic_field_geo_y * math.sin(spacecraft_spacecraft_geo_longitude*math.pi/180) * math.sin(spacecraft_spacecraft_geo_latitude*math.pi/180)',
                                                    'radians',
                                                    'E9.2'),
                              DerivedParameterTuple('mag_geo_horizontal_component',
                                                    # horizontal_component = math.sqrt(eastward_component * eastward_component + northward_component * northward_component)
                                                    'math.sqrt(mag_geo_eastward_component ** 2 + mag_geo_northward_component ** 2)',
                                                    'radians',
                                                    'E9.2'),
                              DerivedParameterTuple('lpw_euv_irradiance_total',
                                                    # euv_irradiance_total = data.euv_irradiance_low + data.euv_irradiance_mid + data.euv_irradiance_lyman_alpha
                                                    'lpw_euv_irradiance_0_1_7_0_nm + lpw_euv_irradiance_17_22_nm + lpw_euv_irradiance_lyman_alpha',
                                                    'W/m^2',
                                                    'E9.2'),
                              DerivedParameterTuple('sep_ion_energy_flux_total',
                                                    # ion_energy_flux_total = data.ion_energy_flux_1 + data.ion_energy_flux_2 + data.ion_energy_flux_3 + data.ion_energy_flux_4
                                                    'sep_ion_energy_flux_30_1000_kev_fov_1_front + sep_ion_energy_flux_30_1000_kev_fov_1_back + sep_ion_energy_flux_30_1000_kev_fov_2_front  + sep_ion_energy_flux_30_1000_kev_fov_2_back',
                                                    'eV/(cm^2 s sr)',
                                                    'E9.2'),
                              DerivedParameterTuple('sep_electron_energy_flux_total',
                                                    # electron_energy_flux_total = data.electron_energy_flux_1 + data.electron_energy_flux_2 + data.electron_energy_flux_3 + data.electron_energy_flux_4
                                                    'sep_electron_energy_flux_30_kev_300_kev_fov_1_front + sep_electron_energy_flux_30_kev_300_kev_fov_1_back + sep_electron_energy_flux_30_kev_300_kev_fov_2_front + sep_electron_energy_flux_30_kev_300_kev_fov_2_back',
                                                    'eV/(cm^2 s sr)',
                                                    'E9.2'),
                              DerivedParameterTuple('spacecraft_spacecraft_illuminated',
                                                    # spacecraft_illuminated = data.spacecraft_mso_x < 0 and (math.sqrt(data.spacecraft_mso_y * data.spacecraft_mso_y + data.spacecraft_mso_z * data.spacecraft_mso_z) < mars_radius)
                                                    '1 if (spacecraft_spacecraft_mso_x < 0 and (math.sqrt(spacecraft_spacecraft_mso_y * spacecraft_spacecraft_mso_y + spacecraft_spacecraft_mso_z * spacecraft_spacecraft_mso_z) < 3396.2)) else 0',
                                                    'boolean',
                                                    'E9.2'),
                              DerivedParameterTuple('spacecraft_mso_latitude_radians',
                                                    # mso_latitude_radians = math.atan(math.sqrt(data.spacecraft_mso_y * data.spacecraft_mso_y + data.spacecraft_mso_z * data.spacecraft_mso_z) / data.spacecraft_mso_x
                                                    'math.atan(math.sqrt(spacecraft_spacecraft_mso_y ** 2 + spacecraft_spacecraft_mso_z ** 2) / spacecraft_spacecraft_mso_x)',
                                                    'radians',
                                                    'E9.2'),
                              DerivedParameterTuple('spacecraft_mso_latitude',
                                                    # mso_latitude = mso_latitude_radians * 180.0 / math.pi
                                                    'spacecraft_mso_latitude_radians * 180.0 / math.pi',
                                                    'radians',
                                                    'E9.2'),
                              DerivedParameterTuple('static_o2plus_flow_velocity_maven_app_magnitude',
                                                    # o2plus_flow_velocity_maven_app_magnitude = math.sqrt(data.o2plus_flow_velocity_maven_app_x ** 2 + data.o2plus_flow_velocity_maven_app_y ** 2 + data.o2plus_flow_velocity_maven_app_z ** 2)
                                                    'math.sqrt(static_o2plus_flow_velocity_maven_app_x ** 2 + static_o2plus_flow_velocity_maven_app_y ** 2 + static_o2plus_flow_velocity_maven_app_z ** 2)',
                                                    'km/s',
                                                    'E9.2'),
                              DerivedParameterTuple('static_o2plus_flow_velocity_mso_magnitude',
                                                    # o2plus_flow_velocity_mso_magnitude = math.sqrt(data.o2plus_flow_velocity_mso_x ** 2 + data.o2plus_flow_velocity_mso_y ** 2 + data.o2plus_flow_velocity_mso_z ** 2)
                                                    'math.sqrt(static_o2plus_flow_velocity_mso_x ** 2 + static_o2plus_flow_velocity_mso_y ** 2 + static_o2plus_flow_velocity_mso_z ** 2)',
                                                    'km/s',
                                                    'E9.2'),
                              DerivedParameterTuple('static_hplus_characteristic_direction_mso_magnitude',
                                                    # hplus_characteristic_direction_mso_magnitude = math.sqrt(data.hplus_characteristic_direction_mso_x ** 2 + data.hplus_characteristic_direction_mso_y ** 2 + data.hplus_characteristic_direction_mso_z ** 2)
                                                    'math.sqrt(static_hplus_characteristic_direction_mso_x ** 2 + static_hplus_characteristic_direction_mso_y ** 2 + static_hplus_characteristic_direction_mso_z ** 2)',
                                                    'km',
                                                    'E9.2'),
                              DerivedParameterTuple('static_dominant_pickup_ion_characteristic_direction_mso_magnitude',
                                                    # dominant_pickup_ion_characteristic_direction_mso_magnitude = math.sqrt(data.dominant_pickup_ion_characteristic_direction_mso_x ** 2 + data.dominant_pickup_ion_characteristic_direction_mso_y ** 2 + data.dominant_pickup_ion_characteristic_direction_mso_z ** 2)
                                                    'math.sqrt(static_dominant_pickup_ion_characteristic_direction_mso_x ** 2 + static_dominant_pickup_ion_characteristic_direction_mso_y ** 2 + static_dominant_pickup_ion_characteristic_direction_mso_z ** 2)',
                                                    'unit vector',
                                                    'E9.2'),  # TODO - Verify this...Seems odd to take magnitude of a unit vector...isn't always 1?
                              DerivedParameterTuple('swia_hplus_flow_total_velocity',
                                                    # hplus_flow_total_velocity = math.sqrt(data.hplus_flow_v_msox * data.hplus_flow_v_msox + data.hplus_flow_v_msoy * data.hplus_flow_v_msoy + data.hplus_flow_v_msoz * data.hplus_flow_v_msoz)
                                                    'math.sqrt(swia_hplus_flow_velocity_mso_x **2 + swia_hplus_flow_velocity_mso_y **2 + swia_hplus_flow_velocity_mso_z ** 2)',
                                                    'km/s',
                                                    'E9.2'),
                              ]

# Tuple Definitions
FormatTuple = namedtuple('FormatTuple', 'instrument,parameter,units,format,notes,column_idx,is_child,is_time,is_derived')
RulesetTuple = namedtuple('RulesetTuple', 'name,identify,process,validate,greedy')
column_pattern = re.compile(r'^# PARAMETER +INSTRUMENT +UNITS .*')
comment_pattern = re.compile(r'^# .*')
data_pattern = re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} .*')

mission_conversions = []


def identify_in_situ_column_line(data, **kwargs):
    # pylint: disable=W0613
    return column_pattern.search(data) is not None


def identify_in_situ_comment_line(data, **kwargs):
    ''' Method used to determine if the provided data can be an identified COMMENT  '''
    # pylint: disable=W0613
    return comment_pattern.search(data) is not None


def identify_in_situ_format_line(data, **kwargs):
    ''' Method used to determine if the provided data can be an identified FORMAT '''
    # pylint: disable=W0613
    if format_pattern_kwarg in kwargs:
        match_result = kwargs[format_pattern_kwarg].match(data)
        if match_result is None:
            return False
        try:
            int(match_result.group('column').strip())
            return True
        except ValueError:
            return False
    return False


def identify_in_situ_data_line(data, **kwargs):
    ''' Method used to determine if the provided data can be an identified DATA '''
    # pylint: disable=W0613
    return data_pattern.search(data) is not None


def process_in_situ_comment_line(data, **kwargs):
    ''' NOOP - COMMENT lines are discarded '''
    # pylint: disable=W0613
    return None


def build_format_pattern(file_to_check):
    '''Method used to find the in_situ_column_line and build the format pattern
    Arguments:
      file_to_check: A string that contains the fully qualified name of the file that contains the in_situ kp data.
    '''
    with open(file_to_check) as f:
        for line in f:
            if identify_in_situ_column_line(line):
                process_in_situ_column_line(line)
                return
    raise Exception('Unable to find column line in file %s' % file_to_check)


def process_in_situ_column_line(data, **kwargs):
    # pylint: disable=W0613
    parameter_location = data.find('PARAMETER')
    instrument_location = data.find('INSTRUMENT')
    units_location = data.find('UNITS')
    column_location = data.find('COLUMN')
    format_location = data.find('FORMAT')
    notes_location = data.find('NOTES')

    # pylint: disable=R1705, R0916
    if parameter_location > 0 and instrument_location > 0 and units_location > 0 and column_location > 0 and format_location > 0 and notes_location > 0:
        format_pattern = re.compile((r'^# (?P<parameter>.{{{0}}})(?P<instrument>.{{{1}}})(?P<units>.{{{2}}})(?P<column>.{{{3}}})(?P<format>.{{{4}}})(?P<notes>.{{{5}}}).*$').format(
            instrument_location - parameter_location,
            units_location - instrument_location,
            column_location - units_location,
            format_location - column_location,
            notes_location - format_location,
            len(data) - notes_location - 2))  # -2 to account for leading # <space>
    else:
        raise Exception('Unable to process column line %s' % data)
    return KeywordArgTuple(format_pattern_kwarg, format_pattern)


def process_in_situ_format_line(data, **kwargs):
    ''' Method used to ingest 1 FORMAT line.  The FORMAT lines are used to determine subsequent DATA positions.
    Arguments:
        data String - 1 line from a KP data file
    Returns:
        FormatTuple - Format for 1 line in the KP file
    '''
    is_child_pattern = re.compile(r'^#  .+')
    m = kwargs[format_pattern_kwarg].match(data)
    instrument = m.group('instrument').strip()
    instrument = instrument if len(instrument) > 0 else 'SPACECRAFT'

    return FormatTuple(instrument,
                       m.group('parameter').strip(),
                       m.group('units').strip(),
                       m.group('format').strip(),
                       m.group('notes').strip(),
                       int(m.group('column').strip()),
                       is_child_pattern.search(data) is not None,
                       m.group('parameter').strip() == 'Time (UTC/SCET)',
                       False
                       )


def process_in_situ_data_line(data, **kwargs):
    ''' Method used to ingest 1 DATA line.
    Arguments:
        data String - 1 line from a KP data file
    Returns:
        [ data_val1,data_val2...data_valn ]
    '''
    # pylint: disable=W0613
    return data.split()


def validate_in_situ_true(data):
    ''' Method used to validate the processed results of a no consequence line '''
    # pylint: disable=W0613
    return True


def validate_in_situ_format(data):
    ''' Method used to validate the processed results of a FORMAT line
    Arguments:
        data : FormatTuple - The processed FormatTuple'''
    return data is not None and int(data.column_idx) is not None


def validate_in_situ_data(data):
    ''' Method used to validate the processed results of a DATA line
    Arguments:
        data : [ ' ',' ',...' ' ] - An array of processed string values
    '''
    time_pattern = re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}')
    return data is not None and len(data) > 1 and time_pattern.search(data[0]) is not None


def get_file_timetag_from_file_name(file_path):
    '''Returns a datetime.datetime object for the time that
    is embedded in the file name.
    '''
    bn = os.path.basename(file_path)
    m = file_pattern.extract_parts([maven_config.kp_regex],
                                   bn,
                                   [file_pattern.general_year_group,
                                    file_pattern.general_month_group,
                                    file_pattern.general_day_group],
                                   file_pattern.time_transforms)
    assert m is not None, 'Key parameter file %s did not match file name pattern' % bn
    yyyy, mm, dd = m.values()
    return datetime(yyyy, mm, dd).replace(tzinfo=pytz.UTC)


def get_file_version(file_path):
    '''Returns the version that is embedded in the file name.'''
    bn = os.path.basename(file_path)
    m = file_pattern.extract_parts([maven_config.kp_regex],
                                   bn,
                                   [file_pattern.general_version_group])
    assert m is not None, 'Key parameter file %s did not match file name pattern' % bn
    version, = m.values()
    return int(version)


def get_file_revision(file_path):
    '''Returns the revision that is embedded in the file name.'''
    bn = os.path.basename(file_path)
    m = file_pattern.extract_parts([maven_config.kp_regex],
                                   bn,
                                   [file_pattern.general_revision_group])
    assert m is not None, 'Key parameter file %s did not match file name pattern' % bn
    revision, = m.values()
    return int(revision)


def add_kp_files_metadata_entry(full_kp_filename, ingest_status, db_session):
    '''Method used to insert an entry into the kp_files_metadata table
    Arguments:
        full_kp_filename - The full KP filename for input into the directory_path and file_name columns
        ingest_status - The kp file ingest status for input into the ingest_status column
    '''
    assert os.path.isfile(full_kp_filename), '%s is not a file' % full_kp_filename
    directory_path, file_name = os.path.split(full_kp_filename)
    file_timetag = get_file_timetag_from_file_name(full_kp_filename)
    version = get_file_version(full_kp_filename)
    revision = get_file_revision(full_kp_filename)
    kp_files_metadata = models.KpFilesMetadata(file_name,
                                               directory_path,
                                               os.path.getsize(full_kp_filename),
                                               'in-situ',
                                               file_timetag,
                                               version,
                                               revision,
                                               ingest_status)
    assert kp_files_metadata is not None, 'failed to create kp files metadata object'
    db_session.add(kp_files_metadata)
    db_session.commit()

    return kp_files_metadata


class insitu_file_processor():
    ''' The file processing class for in-situ files.  This class will read the provided file,
        format the contents and write the database. '''

    # The in-situ file format
    insitu_file_format = [RulesetTuple('Comment', identify_in_situ_comment_line, process_in_situ_comment_line, validate_in_situ_true, False),
                          RulesetTuple('Column', identify_in_situ_column_line, process_in_situ_column_line, validate_in_situ_true, False),
                          RulesetTuple('Comment1', identify_in_situ_comment_line, process_in_situ_comment_line, validate_in_situ_true, False),
                          RulesetTuple('Format', identify_in_situ_format_line, process_in_situ_format_line, validate_in_situ_format, True),
                          RulesetTuple('Comment2', identify_in_situ_comment_line, process_in_situ_comment_line, validate_in_situ_true, False),
                          RulesetTuple('Data', identify_in_situ_data_line, process_in_situ_data_line, validate_in_situ_data, True),
                          ]

    def __init__(self, file_path, db_session, engine, lock, logger=getLogger()):
        '''Sets up the ingest of the data into the database.

        Arguments
            file_path - full path to the file
        '''
        logger.info('Creating new insitu ingester for directory %s', file_path)
        self.logger = logger
        self.file_path = file_path
        self.db_session = db_session
        self.engine = engine
        self.lock = lock
        self.kp_files_metadata = add_kp_files_metadata_entry(self.file_path,
                                                             config.kp_ingest_status_started,
                                                             self.db_session)

    def __enter__(self):
        '''For entering the context manager.'''
        return self

    def __exit__(self, type_, value, traceback):
        '''For exiting the context manager. Commits the
        database transaction.
        '''
        self.db_session.commit()

    def ingest_data(self):
        ''' This is the main ingest method to be used publicly.  The in-situ file is
        read and results are formatted and written to the SDC database.  If
        no errors occur, the meta-data for the in-situ file is updated to
        show a COMPLETE status '''
        try:
            # read in the kp file
            processed_file = self.process_file()
            # generate the kp format and data
            formats, data = self.format_results_row_major(processed_file)
            # send results to the database
            self.write_results(self.kp_files_metadata.id, formats, data)
            # complete
            self.kp_files_metadata.ingest_status = 'COMPLETE'
        except Exception:
            self.kp_files_metadata.ingest_status = 'FAILED'
            raise

    def process_file(self):
        '''
        Uses the rules defined in insitu_file_format to process the various lines
        in the provide in-situ file

        Returns:
        { <processor name> : [ ( <line count> , <processed results> ) ] }
        '''
        with open(self.file_path) as f:
            num_rulesets = len(self.insitu_file_format)
            rule_number = 0
            current_rule_set = self.insitu_file_format[rule_number]
            next_rule_set = self.insitu_file_format[rule_number + 1] if rule_number + 1 < num_rulesets else None
            results = {}
            kwargs = {}

            for line_count, line in enumerate(f, 1):
                # Is the current rule set greedy (e.g. processing regardless of the next ruleset)
                if not current_rule_set.greedy and next_rule_set is not None and next_rule_set.identify(line, **kwargs):
                    current_rule_set, next_rule_set, rule_number = self.increment_rule_set(rule_number)
                elif not current_rule_set.identify(line, **kwargs):
                    current_rule_set, next_rule_set, rule_number = self.increment_rule_set(rule_number)
                processed_results = current_rule_set.process(line, **kwargs)

                assert current_rule_set.validate(processed_results), 'Processor %s identified but failed to validate' % current_rule_set.name

                if isinstance(processed_results, KeywordArgTuple):
                    kwargs[processed_results.key] = processed_results.value
                elif processed_results is not None:
                    results.setdefault(current_rule_set.name, []).append((line_count, processed_results))
        return results

    def increment_rule_set(self, current_rule_number):
        ''' Helper routine used to return the next rule sets to be used for KP data processing.
        If no next rule set exists, None will be returned.
        Arguments:
            current_rule_number Integer - The index of the current rule set being used.
        Returns:
        (
            The new current RulesetTuple or None
            The next RulesetTuple or None
            The next Rule set ID
        )
        '''
        next_rule_number = current_rule_number + 1
        return (self.insitu_file_format[next_rule_number],
                self.insitu_file_format[next_rule_number + 1] if next_rule_number + 1 < len(self.insitu_file_format) else None,
                next_rule_number)

    def get_time_idx(self, format_results):
        '''  Helper routine used to pull the time index from the set of format data
        Arguments:
            format_results: [ (<line number>,FormatTuple) ] - A list of all processed formats
        Returns:
            Integer indicating the column index of the measurement time value or None
        '''
        for next_format in format_results:
            if next_format[1].is_time:
                return int(next_format[1].column_idx) - 1  # 1 based to 0 based
        return None

#     @print_execution_time
    def format_results_row_major(self, raw_results):
        ''' Method used to format the results of process_file.  This method processes the data
        in row major order (time ordered).
        Arguments:
            raw_results: { <processor name> : [ ( <line count> , <processed results> ) ] }
        Returns:
            [
                { format_name : FormatTuple } ,
                { (line#,time) : {format_name : value} }
            ]
        '''
        results = {}
        format_data = raw_results['Format']
        data = raw_results['Data']

        assert format_data is not None and len(format_data) > 0, 'Format data is None or zero length'
        assert data is not None and len(data) > 0, 'Data is None or zero length'

        # ensure all data has consistent length
        self.logger.debug('Number of formats processed : %s, Number of data values %s', len(format_data), len(data[0][1]))

        # iterate over data and format
        formats = {}
        time_idx = self.get_time_idx(format_data)

        assert(time_idx is not None)

        last_parent_format = format_data[0]

        last_column_idx = 0

        # used to maintain a relative child count (e.g like named child parameters will be counted
        child_format_counts = {}

        for next_format_tuple in format_data:
            next_format = next_format_tuple[1]
            self.logger.debug('Processing format %s', next_format)
            last_column_idx = next_format.column_idx if next_format.column_idx > last_column_idx else last_column_idx
            query_parameter_name = ''
            if next_format.is_time:  # is the time format
                continue
            if next_format.is_child and not config.process_quality_data:  # is a child format but we aren't processing child (quality) formats
                continue
            elif next_format.is_child:  # is a child format and we are processing child (quality) formats
                position = child_format_counts.setdefault(next_format.parameter, 0) + 1
                child_format_counts[next_format.parameter] = position
                query_parameter_name = '_'.join([last_parent_format.instrument,
                                                 last_parent_format.parameter,
                                                 next_format.parameter])
            else:
                child_format_counts = {}  # reset child counts
                query_parameter_name = '_'.join([next_format.instrument,
                                                 next_format.parameter])
                last_parent_format = next_format

            # format query_parameter replace + with plus
            query_parameter_name = re.sub('[+]', 'plus', query_parameter_name)
            # format query_parameter replace odd characters with _
            query_parameter_name = re.sub('[^a-zA-Z0-9_]', '_', query_parameter_name)
            # replace > with to
            query_parameter_name = re.sub('>', 'to', query_parameter_name)
            # reduce multiple __ to _
            query_parameter_name = re.sub('_+', '_', query_parameter_name)
            # remove unwanted characters
            query_parameter_name = re.sub('_$', '', query_parameter_name)
            # ensure query_parameter name is < 64 characters (limit on psql column size)
            query_parameter_name = (query_parameter_name[:config.max_query_parameter_name_length]
                                    if len(query_parameter_name) > config.max_query_parameter_name_length
                                    else query_parameter_name)
            # tack on child count
            if next_format.parameter in child_format_counts and child_format_counts[next_format.parameter] > 1:
                if len(query_parameter_name) == config.max_query_parameter_name_length:
                    query_parameter_name = query_parameter_name[:config.max_query_parameter_name_length - 2] + '%02d' % child_format_counts[next_format.parameter]
                else:
                    query_parameter_name = query_parameter_name[:] + '%02d' % child_format_counts[next_format.parameter]

            assert query_parameter_name.lower() not in formats.keys(), '%s was already in %s' % (query_parameter_name.lower(), formats.keys())

            formats[query_parameter_name.lower()] = next_format

        # Add the derived formats
        for next_derived_parameter in derived_parameter_formulas:
            last_column_idx += 1
            query_parameter_name = (next_derived_parameter.name[:config.max_query_parameter_name_length]
                                    if len(next_derived_parameter.name) > config.max_query_parameter_name_length
                                    else next_derived_parameter.name)
            formats[query_parameter_name] = FormatTuple('Derived',
                                                        next_derived_parameter.name,
                                                        next_derived_parameter.units,
                                                        next_derived_parameter.format,
                                                        'Derived via %s' % next_derived_parameter.formula,
                                                        last_column_idx,
                                                        False,
                                                        False,
                                                        True
                                                        )
        format_data_len = len(format_data)
        for datum in data:
            if format_data_len != len(datum[1]):
                self.logger.error("format mismatch on line %s", datum[0])
                raise Exception('# formats processed {0} !='
                                '# data values {1} on line number: {2}'.format(format_data_len, len(datum[1]), datum[0]))
            time = datum[1][time_idx]
            results[(datum[0], time)] = {}

            for key in [k for k in formats if not formats[k].is_derived]:
                results[(datum[0], time)][key] = datum[1][formats[key].column_idx - 1]  # 1 based to 0 based

            # Generate derived parameters
            for next_derived_parameter in derived_parameter_formulas:
                result = self.derive_parameter(next_derived_parameter, results[(datum[0], time)])
                query_parameter_name = (next_derived_parameter.name[:config.max_query_parameter_name_length]
                                        if len(next_derived_parameter.name) > config.max_query_parameter_name_length
                                        else next_derived_parameter.name)
                results[(datum[0], time)][query_parameter_name] = str(result)

        return formats, results

    def derive_parameter(self, derive_parm_tuple, row_values):
        ''' Helper method used to execute the formula provided in derive_parm_tuple
        Arguments:
            derive_parm_tuple : The Derived parameter meta-data used to derive the in-situ parameter
            row_values : The set of existing in-situ values
        Returns:
            The derived value after the provided calculation has occurred.
        '''
        # pylint: disable=W0123
        # Removes warning for eval()
        row_values_as_floating_point = {}
        for key, value in row_values.items():
            try:
                float_value = float(value)
                if math.isnan(float_value):
                    continue
                row_values_as_floating_point[key] = float_value
            except ValueError:
                continue
        try:
            return eval(derive_parm_tuple.formula, None, row_values_as_floating_point)
        except NameError:
            return float('nan')

    def convert_a16_datavalue(self, format_id, a16_datavalue):
        ''' Helper routine used to convert non-numeric data into floating point values
        Arguments:
            format_id: String - A16 Format ID value
            a16_datavalue: String - A16 formatted value for conversion (e.g. 'D' associated with 0, 'P' with 1)
        Returns: A converted string from the a16_conversions or a16_default conversions dictionary ('nan')
        Default values are no longer supported and raise an exception if format_id is not mapped
        '''
        if format_id in config.a16_conversions:
            # Searches for the format_id in the a16_conversions dictionary or as a default 'nan'
            if a16_datavalue in config.a16_conversions[format_id]:
                return config.a16_conversions[format_id][a16_datavalue]
            if a16_datavalue.lower() in config.a16_default_conversions:
                return config.a16_default_conversions[a16_datavalue.lower()]
            
            if a16_datavalue not in mission_conversions:
                mission_conversions.append(a16_datavalue)
                self.logger.warning('Unable to find a conversion for %s.  Returning NaN', a16_datavalue)
            return float('nan')

        # format_id is not part of the A16 format ID dictionaries and could not be converted
        raise Exception("Data value '%s' could not be converted explicitly from format ID '%s'" % (a16_datavalue, format_id))

#     @print_execution_time
    def insert_results(self, insert_list):
        '''Helper routine used by write_results to commit KP data
           to the SDC database.
        Arguments:
            insert_list - A list of dictionaries to insert into the
                          database.
        '''
        len_insert_list = len(insert_list)
        batch_size = config.batch_size
        batch_list = [insert_list[i:i + batch_size]
                      for i in range(0, len_insert_list, batch_size)]
        for batch in batch_list:
            if len(batch) > 0:
                with self.engine.begin() as connection:
                    connection.execute(models.InSituKeyParametersData.__table__.insert(),
                                       batch)

#     @print_execution_time
    def write_results(self, file_metadata_id, format_data, instrument_data):
        ''' Method used to commit the KP data to the SDC database.
        Arguments:
        file_metadata_id:
            Integer value that represents the foreign key to the kp_files_metadata table
        format_data:
            { format_name : FormatTuple }
        instrument_data:
            [ (line#,time) : {format_name : value} ]
         '''
        format_db_idx = {}
        # For all keys, create query parameters if they don't already exist
        for key in format_data:
            column_format = self.db_session.query(models.InSituKpQueryParameter).filter(models.InSituKpQueryParameter.query_parameter == key).first()

            if column_format is None:
                format_data_val = format_data[key]
                column_format = models.InSituKpQueryParameter(key,
                                                                format_data_val.instrument,
                                                                format_data_val.parameter,
                                                                format_data_val.format,
                                                                format_data_val.units,
                                                                format_data_val.notes)

                self.db_session.add(column_format)
                self.db_session.commit()

            format_db_idx[column_format.id] = key  # map db index to format key

        core_insert_list = []
        for data_key in instrument_data:
            sample_time_utc = datetime.strptime(data_key[1], '%Y-%m-%dT%H:%M:%S')
            sample_time_utc = sample_time_utc.replace(tzinfo=pytz.utc)
            sample_line_number = int(data_key[0])
            sample_metadata_id = file_metadata_id

            data_values = instrument_data[data_key]

            for format_key in format_db_idx:
                format_id = format_db_idx[format_key]
                data_value = data_values[format_id]

                if format_data[format_id].format == config.a16_format:
                    data_value = float(self.convert_a16_datavalue(format_id, data_value))
                else:
                    data_value = float(data_value)

                if not math.isnan(data_value):
                    core_insert_list.append(
                        {"kp_files_metadata_id": sample_metadata_id,
                         "timetag": sample_time_utc,
                         "file_line_number": sample_line_number,
                         "data_value": data_value,
                         "in_situ_kp_query_parameters_id": format_key
                         })

        num_threads = config.num_threads
        if num_threads < 2:
            self.insert_results(core_insert_list)
            return

        len_core_insert_list = len(core_insert_list)
        share_size, left_over = divmod(len_core_insert_list, num_threads)
        divided_insert_list = [core_insert_list[i:i + share_size]
                               for i in range(0, len_core_insert_list, share_size)]

        if left_over:
            divided_insert_list[0].extend(divided_insert_list.pop())

        thread_list = [Thread(target=self.insert_results, args=([l]))
                       for l in divided_insert_list]

        for t in thread_list:
            t.start()
        for t in thread_list:
            t.join()


def is_data_line(line):
    '''Returns True if the line is a data line.'''
    return not line.strip().startswith('#')
