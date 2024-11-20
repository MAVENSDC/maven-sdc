'''
Created on Dec 9, 2014

@author: bstaley
'''
import unittest
import os
from multiprocessing import Lock
from shutil import rmtree
from maven_in_situ_kp_file_ingester.in_situ_kp_file_processor import insitu_file_processor, derived_parameter_formulas
from maven_in_situ_kp_file_ingester import config, in_situ_kp_file_processor
from maven_database import database, models
from tests.maven_test_utilities.file_system import get_temp_root_dir
from tests.maven_test_utilities.db_utils import delete_data
from tests.maven_test_utilities import generate_format
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'


class InsituProcessorTestCase(unittest.TestCase):

    def setUp(self):
        self.num_samples = 4
        self.parent_formats = 0
        self.child_formats = 0
        self.child_format_name = 'child'
        self.derived_formats = len(derived_parameter_formulas)
        self.test_dir = get_temp_root_dir()
        self.file_name = self.test_dir + '/mvn_kp_insitu_20141011_v01_r01.tab'

        # Providing dictionary conversions for dummy test child formats
        config.a16_conversions['instrument_parameter_with_a_name_longer_than_63_character_child'] = {'I': 0,
                                                                                                     'O': 1,
                                                                                                     'I/O': 2}

        config.a16_conversions['instrument_parameter_with_a_name_longer_than_63_character_chi02'] = {'I': 0,
                                                                                                     'O': 1,
                                                                                                     'I/O': 2}

        with open(self.file_name, 'w') as f:
            # Add the column format line
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'PARAMETER', 'INSTRUMENT', 'UNITS', 'COLUMN #', 'FORMAT', 'NOTES'))
            f.write('# Some comment line\n')
            # Add the time format
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Time (UTC/SCET)', 'instrument', 'unit', 1, 'format', 'notes'))
            # Add derived formats required by derived parameters
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Spacecraft GEO X', '      ', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Spacecraft GEO Y', '      ', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Spacecraft GEO Z', '      ', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Spacecraft MSO X', '      ', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Spacecraft MSO Y', '      ', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Spacecraft MSO Z', '      ', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Spacecraft GEO Longitude', '      ', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Spacecraft GEO Latitude', '      ', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Magnetic Field MSO X', 'MAG', 'nT', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Magnetic Field MSO Y', 'MAG', 'nT', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Magnetic Field MSO Z', 'MAG', 'nT', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Magnetic Field GEO X', 'MAG', 'nT', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Magnetic Field GEO Y', 'MAG', 'nT', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Magnetic Field GEO Z', 'MAG', 'nT', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'EUV irradiance (0.1 - 7.0 nm)', 'LPW', 'W/m^2', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'EUV irradiance (17 - 22 nm)', 'LPW', 'W/m^2', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'EUV irradiance (Lyman-alpha)', 'LPW', 'W/m^2', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Ion Energy Flux (30-1000 keV), FOV 1-Front', 'SEP', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Ion Energy Flux (30-1000 keV), FOV 1-Back', 'SEP', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Ion Energy Flux (30-1000 keV), FOV 2-Front', 'SEP', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Ion Energy Flux (30-1000 keV), FOV 2-Back', 'SEP', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Electron Energy Flux (30 keV - 300 keV) - FOV 1-Front', 'SEP', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Electron Energy Flux (30 keV - 300 keV) - FOV 1-Back', 'SEP', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Electron Energy Flux (30 keV - 300 keV) - FOV 2-Front', 'SEP', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Electron Energy Flux (30 keV - 300 keV) - FOV 2-Back', 'SEP', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'O2+ flow velocity MSO X', 'STATIC', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'O2+ flow velocity MSO Y', 'STATIC', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'O2+ flow velocity MSO Z', 'STATIC', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'O2+ flow velocity MAVEN_APP X', 'STATIC', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'O2+ flow velocity MAVEN_APP Y', 'STATIC', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'O2+ flow velocity MAVEN_APP Z', 'STATIC', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'H+ characteristic direction MSO X', 'STATIC', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'H+ characteristic direction MSO Y', 'STATIC', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'H+ characteristic direction MSO Z', 'STATIC', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Dominant pickup ion characteristic direction MSO X', 'STATIC', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Dominant pickup ion characteristic direction MSO Y', 'STATIC', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'Dominant pickup ion characteristic direction MSO Z', 'STATIC', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'H+ flow velocity MSO X', 'SWIA', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'H+ flow velocity MSO Y', 'SWIA', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'H+ flow velocity MSO Z', 'SWIA', 'km', self.parent_formats + 1, 'format', 'notes'))
            self.parent_formats += 1
            f.write(generate_format.inSituGenerateFormatLine(
                None, 'parameter_with_a_name_longer_than_63_character', 'instrument', 'unit', self.parent_formats + 1, 'format', 'notes'))

            # 2 similarly named children
            self.child_formats += 1
            f.write(generate_format.inSituGenerateFormatLine('include indent', self.child_format_name,
                                                             'instrument', 'unit', self.parent_formats + self.child_formats + 1, 'A16', 'notes'))

            self.child_formats += 1
            f.write(generate_format.inSituGenerateFormatLine('include indent', self.child_format_name,
                                                             'instrument', 'unit', self.parent_formats + self.child_formats + 1, 'A16', 'notes'))

            # Format ID 'A16' tests for the non-numerical values in
            # a16_conversions dictionary to associated floating point data
            # values

            data = ['I' if i >= self.parent_formats else i for i in range(
                self.parent_formats + self.child_formats)]
            nan_data = [float('nan') for i in range(
                self.parent_formats + self.child_formats)]
            # NaN for Magnetic Field MSO X which affects 2 derived parameters
            mixed_data = [float('nan') if i == 8 else i for i in range(
                self.parent_formats + self.child_formats)]
            mixed_data = [
                'I' if i >= self.parent_formats else i for i in mixed_data]
            a16_data = ['I' if i >= self.parent_formats else i for i in range(
                self.parent_formats + self.child_formats)]
            # write a comment line
            f.write('# \n')
            for i in range(0, self.num_samples - 3):
                f.write(generate_format.inSituGenerateDataLine(
                    time_utilities.utc_now(), data))
            f.write(generate_format.inSituGenerateDataLine(
                time_utilities.utc_now(), nan_data))
            f.write(generate_format.inSituGenerateDataLine(
                time_utilities.utc_now(), mixed_data))
            f.write(generate_format.inSituGenerateDataLine(
                time_utilities.utc_now(), a16_data))

            self.total_formats = self.parent_formats + \
                self.child_formats + self.derived_formats
            self.total_no_child_formats = self.parent_formats + \
                self.derived_formats

            # -3 because of nan_data, mixed_data and a16_data (3) where 'self.num_samples - 3' results in the number of rows generated by the data variable
            data_expected = (self.num_samples - 3) * \
                (len(data) + len(derived_parameter_formulas))
            # -2 because NaN for Magnetic Field MSO X which affects 2
            mixed_data_expected = len(
                [a for a in mixed_data if str(a) != 'nan']) + len(derived_parameter_formulas) - 2
            a16_data_expected = len(a16_data) + len(derived_parameter_formulas)

            self.expected_values = data_expected + \
                mixed_data_expected + a16_data_expected

            # -6 because we have 3 valid (e.g non NaN) rows that each contain 2 child formats
            self.expected_no_child_values = self.expected_values - 6

    def tearDown(self):
        delete_data()
        rmtree(self.test_dir)
        self.assertFalse(os.path.isdir(self.test_dir))

    def testMultipleSameNamedChildFormats(self):
        '''Test to ensure same name child formats are handled'''
        with insitu_file_processor(self.file_name, database.db_session, database.engine, Lock()) as processor:
            processor.ingest_data()
            child_formats = models.InSituKpQueryParameter.query.filter(models.InSituKpQueryParameter.kp_column_name.like(
                '%%%s%%' % self.child_format_name)).order_by(models.InSituKpQueryParameter.id).all()
            self.assertTrue(len(child_formats) > 1)
            self.assertIn(
                self.child_format_name, child_formats[0].query_parameter)
            fmt_cnt = 1
            self.assertNotIn(
                '%02d' % fmt_cnt, child_formats[0].query_parameter)

            for child_format in child_formats[1:]:
                fmt_cnt += 1
                self.assertIn('%02d' % fmt_cnt, child_format.query_parameter)

    def testNoFormatColumnContainsMoreThan63Characters(self):
        '''Test to ensure all generated query_parametrs are less then the configured maximum
        length'''
        with insitu_file_processor(self.file_name, database.db_session, database.engine, Lock()) as processor:
            processor.ingest_data()

            all_formats = models.InSituKpQueryParameter.query.all()

            for next_format in all_formats:
                self.assertTrue(
                    len(next_format.query_parameter) <= config.max_query_parameter_name_length)

    @unittest.skip('skipping')
    def testPopulateDevInsitu(self):
        ''' Unit test to exercise the in-situ file processor '''

        with insitu_file_processor('/home/bstaley/tmp/mvn_kp_insitu_20141011_v01_r01.tab', database.db_session, database.engine, Lock()) as processor:
            processor.ingest_data()

    # @unittest.skip('skipping')
    def testInSituProcessor(self):
        ''' Unit test to exercise the in-situ file processor '''

        with insitu_file_processor(self.file_name, database.db_session, database.engine, Lock()) as processor:
            processor.ingest_data()

            num_insitu_kp_data = models.InSituKeyParametersData.query.count()
            num_insitu_kp_formats = models.InSituKpQueryParameter.query.count()

            if config.process_quality_data:
                self.assertEqual(self.expected_values, num_insitu_kp_data)
                self.assertEqual(self.total_formats, num_insitu_kp_formats)
            else:
                self.assertEqual(
                    self.expected_no_child_values, num_insitu_kp_data)
                self.assertEqual(
                    self.total_no_child_formats, num_insitu_kp_formats)

            # Get a full row of data
            kp_data_vals = [x[0] for x in database.db_session.query(models.InSituKeyParametersData.data_value).filter(
                models.InSituKeyParametersData.file_line_number == self.parent_formats + self.child_formats + 5).all()]

            for i in [float(f) for f in range(self.num_samples)]:
                self.assertTrue(
                    i in kp_data_vals, 'Was not able to find %s in the test KP values!' % i)

    # @unittest.skip('skipping')
    def testProcessQuality(self):
        ''' Test to check the ability to process quality formats '''
        config.process_quality_data = True

        self.runProcessorCheckResults(
            True, self.total_formats, self.expected_values)

    # @unittest.skip('skipping')
    def testProcessNoQuality(self):
        ''' Test to check the ability to ignore quality formats '''
        config.process_quality_data = False

        self.runProcessorCheckResults(
            True, self.total_no_child_formats, self.expected_no_child_values)

    def testNonNumericalConversion(self):
        ''' Test to check that non-numerical values in a16_conversions are converted to floating point '''
        # Creates a dictionary of the actual a16 conversion values
        a16_dict = {}
        for data in config.a16_conversions.values():
            a16_dict.update(data)

        # Creates a dictionary of the values returned by convert_a16_datavalue
        # method
        converted_values = {}
        format_id = config.a16_conversions.keys()
        with insitu_file_processor(self.file_name, database.db_session, database.engine, Lock()) as processor:
            for format_val in format_id:
                key = config.a16_conversions[format_val].keys()
                for key_val in key:
                    co_val = processor.convert_a16_datavalue(
                        format_val, key_val)
                    converted_values[key_val] = co_val

        # Test to compare the values that convert_a16_datavalue returns to
        # actual a16_conversions values
        self.assertEqual(converted_values, a16_dict)
    
    def testBuildFormatPattern(self):
        format_filename = 'testing_file_pattern.txt'
        file_path = os.path.join(self.test_dir, format_filename)
        
        with self.assertRaises(IOError):
            in_situ_kp_file_processor.build_format_pattern(file_path)

        with open(file_path, 'w') as f:
            f.write('testing')
        self.assertTrue(os.path.isfile(file_path))
        
        with self.assertRaises(Exception):
            in_situ_kp_file_processor.build_format_pattern(file_path)

    def runProcessorCheckResults(self, process_quality, expected_formats, expected_data):
        ''' Method used to run the insitu_file_processor.ingest_data() and check the generated formats and data '''
        with insitu_file_processor(self.file_name, database.db_session, database.engine, Lock()) as processor:
            processor.ingest_data()

            num_insitu_kp_data = models.InSituKeyParametersData.query.count()
            num_insitu_kp_formats = models.InSituKpQueryParameter.query.count()

            self.assertEqual(expected_data, num_insitu_kp_data)
            self.assertEqual(expected_formats, num_insitu_kp_formats)
