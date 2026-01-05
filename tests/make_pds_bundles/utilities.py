'''
Created on Mar 2, 2015

@author: bstaley
'''
import os
import pytz
from datetime import datetime
from maven_data_file_indexer import utilities as idx_utils
from maven_database.models import ScienceFilesMetadata
from maven_database import db_session
from maven_utilities import file_pattern, maven_config, utilities as utils_utilities
from maven_utilities.time_utilities import to_utc_tz
from maven_utilities import constants, time_utilities
os.environ[constants.python_env] = 'testing'


def get_metadata(test_science_files, root_dir):
    ''' Method used to generate metadata for the set of provided science files '''
    metadata_results = []
    for f in test_science_files:
        next_result = generate_metadata_for_science_file(f, root_dir)
        assert next_result is not None, 'was not able to generate metadata for %s' % f
        next_root_path = os.path.join(next_result.directory_path, next_result.file_name)
        metadata_results.append((next_result, next_root_path))
    return metadata_results


def generate_metadata_for_science_file(filename, rd):
    '''Returns the metadata embedded in the quicklook, KP, level 1, 2, or 3 file name.
    Argument
        filename - name of a quicklook, KP or level 1, 2, or 3 file
    '''
    bn = os.path.basename(filename)
    m = file_pattern.extract_parts([maven_config.science_regex,
                                    maven_config.kp_regex,
                                    maven_config.label_regex,
                                    maven_config.euv_regex,
                                    maven_config.euv_flare_regex,
                                    maven_config.euv_flare_catalog_regex],
                                   bn,
                                   [file_pattern.general_instrument_group,
                                    file_pattern.general_level_group,
                                    file_pattern.general_description_group,
                                    file_pattern.general_year_group,
                                    file_pattern.general_month_group,
                                    file_pattern.general_day_group,
                                    file_pattern.general_hh_group,
                                    file_pattern.general_mm_group,
                                    file_pattern.general_hhmmss_group,
                                    file_pattern.general_version_group,
                                    file_pattern.general_revision_group,
                                    file_pattern.general_extension_group, 
                                    file_pattern.general_flare_class],
                                   file_pattern.time_transforms,
                                   handle_missing_parts=True)
    if m is None:
        return None

    
    if m['hhmmss'] is None:
        m['hhmmss'] = (None, None, None)

    # unpack m
    instrument, level, descriptor, year, month, day, hh, mm, ss, file_version, file_revision, extension, flare_class = m.values()

    version = file_version if file_version is not None else '1'
    revision = file_revision if file_revision is not None else '0'

    file_name = bn
    # NO FILE SIZE file_size = os.path.getsize(filename)
    if hh is None:
        hh, mm, ss = 0, 0, 0
    if ss is None or ss == (None, None, None):
        ss = 0
    if level is None:
        level = 'na'
    if month is None:
        month, day = 1, 1

    hh = int(hh)
    mm = int(mm)
        
    timetag = datetime(year, month, day, hh, mm, ss)
    timetag_utc = to_utc_tz(timetag)
    plan = None
    orbit = None
    mode = None
    data_type = None

    if descriptor is not None and descriptor != '':
        descriptor = descriptor.lstrip('_')
        tkns = descriptor.split('-')
        if len(tkns) > 0:
            plan = tkns[0]
        if len(tkns) > 1:
            orbit = tkns[1]
        if len(tkns) > 2:
            mode = tkns[2]
        if len(tkns) > 3:
            data_type = tkns[3]

    directory_path = os.path.join(rd, instrument, level, str(timetag_utc.year), str(timetag_utc.month))

    return idx_utils.ScFileMetadata(directory_path,
                                    file_name,
                                    utils_utilities.get_file_root_plus_extension(file_name, file_name),
                                    0,
                                    instrument,
                                    level,
                                    descriptor,
                                    timetag_utc,
                                    time_utilities.utc_now(),
                                    utils_utilities.get_absolute_version(int(version), int(revision)),
                                    int(version),
                                    int(revision),
                                    extension,
                                    plan,
                                    orbit,
                                    mode,
                                    data_type,
                                    flare_class=flare_class)


def generate_metadata_for_metadata_file(filename, rd):
    '''Returns the metadata embedded in the quicklook, KP, level 1, 2, or 3 file name.
    Argument
        filename - name of a quicklook, KP or level 1, 2, or 3 file
    '''
    bn = os.path.basename(filename)
    m = file_pattern.extract_parts([maven_config.metadata_index_regex],
                                   bn,
                                   [file_pattern.general_instrument_group,
                                    file_pattern.general_level_group,
                                    maven_config.meta_type_group,
                                    maven_config.meta_description,
                                    file_pattern.general_year_group,
                                    file_pattern.general_month_group,
                                    file_pattern.general_day_group,
                                    file_pattern.general_hhmmss_group,
                                    file_pattern.general_extension_group,
                                    file_pattern.general_gz_extension_group],
                                   file_pattern.time_transforms)
    if m is None:
        return None

    # unpack m
    instrument, level, meta_type, descriptor, year, month, day, (hh, mm, ss), extension, _ = m.values()

    version = 1
    revision = 0
    plan = 'metadata'

    file_name = bn
    # NO FILE SIZE file_size = os.path.getsize(filename)
    if hh is None:
        hh, mm, ss = 0, 0, 0
    timetag = datetime(year, month, day, hh, mm, ss, tzinfo=pytz.UTC)

    orbit = None
    mode = None
    flare_class=None

    directory_path = os.path.join(rd, instrument, 'metadata')

    return idx_utils.ScFileMetadata(directory_path,
                                    file_name,
                                    utils_utilities.get_file_root_plus_extension(file_name, file_name),
                                    0,
                                    instrument,
                                    level,
                                    descriptor,
                                    timetag,
                                    time_utilities.utc_now(),
                                    utils_utilities.get_absolute_version(int(version), int(revision)),
                                    int(version),
                                    int(revision),
                                    extension,
                                    plan,
                                    orbit,
                                    mode,
                                    meta_type,
                                    flare_class=flare_class)


def populate_science_metadata(metadata_list):
    for metadata in metadata_list:
        m = ScienceFilesMetadata(metadata.file_name,
                                 utils_utilities.get_file_root_plus_extension(metadata.file_name, metadata.file_name),
                                 metadata.directory_path,
                                 metadata.file_size,
                                 metadata.instrument,
                                 metadata.level,
                                 metadata.timetag,
                                 absolute_version=utils_utilities.get_absolute_version(metadata.version, metadata.revision),
                                 version=metadata.version,
                                 revision=metadata.revision,
                                 mod_date=metadata.mod_date,
                                 descriptor=metadata.descriptor,
                                 file_extension=metadata.file_extension,
                                 plan=metadata.plan,
                                 orbit=metadata.orbit,
                                 mode=metadata.mode,
                                 data_type=metadata.data_type)
        db_session.add(m)
        db_session.commit()
