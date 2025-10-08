# pylint: disable=E1101
'''
Created on Mar 14, 2016

@author: bstaley
'''

from collections import namedtuple
import os
import logging
from datetime import datetime, timedelta
import pytz

from maven_data_file_indexer.maven_file_indexer import console_logger
from maven_utilities import time_utilities, anc_config, maven_config, file_pattern, utilities as util_utilities
from maven_database.database import db_session
from maven_database.models import ScienceFilesMetadata, AncillaryFilesMetadata
from maven_orbit.maven_orbit import get_orbit_perigee_time
from sqlalchemy.exc import IntegrityError

# use MAVEN epoch
abs_version_epoch_dt = datetime(2013, 11, 18, 0, tzinfo=pytz.UTC)

logger = logging.getLogger('maven.maven_data_file_indexer.utilities.log')

L0Metadata = namedtuple('L0Metadata', ['directory_path',
                                       'file_name',
                                       'file_root',
                                       'file_size',
                                       'instrument',
                                       'grouping',
                                       'level',
                                       'timetag',
                                       'mod_date',
                                       'version', ])

ScFileMetadata = namedtuple('ScFileMetadata', ['directory_path',
                                               'file_name',
                                               'file_root',
                                               'file_size',
                                               'instrument',
                                               'level',
                                               'descriptor',
                                               'timetag',
                                               'mod_date',
                                               'absolute_version',
                                               'version',
                                               'revision',
                                               'file_extension',
                                               'plan',
                                               'orbit',
                                               'mode',
                                               'data_type', 'flare_class'])

AncFileMetadata = namedtuple('AncFileMetadata', ['directory_path',
                                                 'file_name',
                                                 'file_root',
                                                 'base_name',
                                                 'file_size',
                                                 'product',
                                                 'file_extension',
                                                 'start_date',
                                                 'end_date',
                                                 'mod_date',
                                                 'version', ])


def convert_ydoy_to_datetime(year, day):
    '''Convert a year (YYYY) and day (day-of-year) to a datetime in UTC at the
    beginning of the day.'''
    # start on day 1 of the year, add the day-of-year offset
    dt = datetime(year, 1, 1) + timedelta(day - 1)
    return pytz.utc.localize(dt)


def is_science_metadata(fullpath):
    bn = os.path.basename(fullpath)
    parts = file_pattern.extract_parts([maven_config.l0_regex,
                                        maven_config.ql_regex,
                                        maven_config.metadata_index_regex,
                                        maven_config.science_regex,
                                        maven_config.kp_regex,
                                        maven_config.sep_anc_regex,
                                        maven_config.label_regex,
                                        maven_config.euv_l2b_regex,
                                        maven_config.euv_regex,
                                        maven_config.euv_flare_regex,
                                        maven_config.euv_flare_catalog_regex,
                                        maven_config.euv_l4_regex],
                                       bn,
                                       [])
    return parts is not None


def is_ancillary_metadata(fullpath):
    bn = os.path.basename(fullpath)
    parts = file_pattern.extract_parts(anc_config.ancillary_regex_list,
                                       bn,
                                       [])
    return parts is not None


def generate_metadata_for_l0_file(fullpath_list):
    for f in fullpath_list:
        next_result = get_metadata_for_l0_file(f)
        if next_result:
            yield next_result


def get_metadata_for_l0_file(fullpath):
    '''Returns a list of the metadata embedded in the level 0 file names.

    Argument
        fullpath_list - A list of full paths of  level 0 files
    '''
    bn = os.path.basename(fullpath)
    parts = file_pattern.extract_parts([maven_config.l0_regex],
                                       bn,
                                       [file_pattern.general_instrument_group,
                                        maven_config.l0_grouping_group,
                                        file_pattern.general_level_group,
                                        file_pattern.general_year_group,
                                        file_pattern.general_month_group,
                                        file_pattern.general_day_group,
                                        file_pattern.general_version_group],
                                       file_pattern.time_transforms,
                                       )
    if parts is None:
        return None
    if not os.path.isfile(fullpath):
        logger.debug('%s does not exist', fullpath)
        return None

    instrument, grouping, level, year, month, day, version = parts.values()
    directory_path = os.path.dirname(fullpath)
    file_name = bn
    version = version or -1
    file_size = os.path.getsize(fullpath)
    timetag = datetime(year, month, day, 0, 0, 0)
    timetag_utc = time_utilities.to_utc_tz(timetag)

    return L0Metadata(directory_path=directory_path,
                      file_name=file_name,
                      file_root=util_utilities.get_file_root_plus_extension(file_name, file_name),
                      file_size=file_size,
                      instrument=instrument,
                      grouping=grouping,
                      level=level,
                      timetag=timetag_utc,
                      mod_date=timetag_utc,
                      version=int(version))


def generate_metadata_for_ql_file(fullpath_list):
    for f in fullpath_list:
        next_result = get_metadata_for_ql_file(f)
        if next_result:
            yield next_result


def get_metadata_for_ql_file(fullpath):
    '''Returns the metadata embedded in the ql/summary plot files.

    Argument
        fullpath_list - A list of full paths of metadata files.
    '''
    directory, bn = os.path.split(fullpath)
    _, parent = os.path.split(directory)

    parts = file_pattern.extract_parts([maven_config.ql_regex],
                                       bn,
                                       [file_pattern.general_instrument_group,
                                        file_pattern.general_level_group,
                                        file_pattern.general_year_group,
                                        file_pattern.general_month_group,
                                        file_pattern.general_day_group,
                                        maven_config.ql_orbit_group,
                                        file_pattern.general_extension_group],
                                       file_pattern.time_transforms)
    if parts is None:  # No matches
        return None

    instrument, level, year, month, day, orbit_group, extension = parts.values()
    file_size = os.path.getsize(fullpath)
    try:
        # extract time info
        dt = datetime(year, month, day, tzinfo=pytz.UTC)
    except Exception:
        logger.exception('Unable to determine datetime of file %s.  Parent directory was %s.  File will not be indexed', fullpath, parent)
        return None

    # defaulted until this info is provided
    version = 1
    revision = 0
    abs_version = (dt - abs_version_epoch_dt).total_seconds()
    plan = 'quicklook'

    return ScFileMetadata(directory_path=directory,
                          file_name=bn,
                          file_root=bn,
                          file_size=file_size,
                          instrument=instrument,
                          level=level,
                          descriptor=orbit_group,
                          timetag=dt,
                          absolute_version=abs_version,
                          version=version,
                          revision=revision,
                          file_extension=extension,
                          plan=plan,
                          mod_date=util_utilities.get_mtime(fullpath),
                          orbit=None,
                          mode=None,
                          data_type=None,
                          flare_class=None)


def generate_metadata_for_metadata_file(fullpath_list):
    for f in fullpath_list:
        next_result = get_metadata_for_metadata_file(f)
        if next_result:
            yield next_result


def get_metadata_for_metadata_file(fullpath):
    '''Returns the metadata embedded in the metadata files.

    Argument
        fullpath_list - A list of full paths of metadata files.
    '''
    directory, bn = os.path.split(fullpath)
    _, parent = os.path.split(directory)

    parts = file_pattern.extract_parts([maven_config.metadata_index_regex],
                                       bn,
                                       [file_pattern.general_instrument_group,
                                        file_pattern.general_level_group,
                                        maven_config.meta_type_group,
                                        maven_config.meta_description,
                                        file_pattern.general_year_group,
                                        file_pattern.general_month_group,
                                        file_pattern.general_day_group,
                                        file_pattern.general_hhmmss_group,
                                        file_pattern.general_version_group,
                                        file_pattern.general_revision_group,
                                        file_pattern.general_extension_group,
                                        file_pattern.general_gz_extension_group],
                                       file_pattern.time_transforms)
    if parts is None:  # No matches
        return None
    instrument, level, _, description, year, month, day, (hh, mm, ss), ver, rev, extension, _ = parts.values()
    file_size = os.path.getsize(fullpath)
    try:
        # extract time info
        dt = datetime(year, month, day, hh, mm, ss)
        dt_utc = time_utilities.to_utc_tz(dt)
    except Exception:
        logger.exception('Unable to determine datetime of file %s.  Parent directory was %s.  File will not be indexed', fullpath, parent)
        return None

    # defaulted until this info is provided
    version = 1 if ver is None else int(ver)
    revision = 0 if rev is None else int(rev)
    abs_version = (dt_utc - abs_version_epoch_dt).total_seconds()
    plan = 'metadata'

    return ScFileMetadata(directory_path=directory,
                          file_name=bn,
                          file_root=bn,
                          file_size=file_size,
                          instrument=instrument,
                          level=level,
                          descriptor=description,
                          timetag=dt_utc,
                          absolute_version=abs_version,
                          version=version,
                          revision=revision,
                          file_extension=extension,
                          plan=plan,
                          mod_date=util_utilities.get_mtime(fullpath),
                          orbit=None,
                          mode=None,
                          data_type=None,
                          flare_class=None)


def generate_metadata_for_science_file(fullpath_list):
    '''Returns the metadata embedded in the quicklook, KP, level 1, 2, or 3 file name.

    Argument
        fullpath_list - A list of full paths of quicklook, KP or level 1, 2, or 3 files.
    '''
    for f in fullpath_list:
        next_result = get_metadata_for_science_file(f)
        if next_result:
            yield next_result


def get_metadata_for_science_file(fullpath):
    '''Returns the metadata embedded in the quicklook, KP, level 1, 2, or 3 file name.

    Argument
        fullpath_list - A list of full paths of quicklook, KP or level 1, 2, or 3 files.
    '''
    bn = os.path.basename(fullpath)

    parts = file_pattern.extract_parts([maven_config.sep_anc_regex,
                                        maven_config.science_regex,
                                        maven_config.kp_regex,
                                        maven_config.label_regex,
                                        maven_config.euv_l2b_regex,
                                        maven_config.euv_regex,
                                        maven_config.euv_flare_regex,
                                        maven_config.euv_flare_catalog_regex,
                                        maven_config.euv_l4_regex],
                                       bn,
                                       [file_pattern.general_instrument_group,
                                        file_pattern.general_level_group,
                                        file_pattern.general_description_group,
                                        file_pattern.general_year_group,
                                        file_pattern.general_month_group,
                                        file_pattern.general_day_group,
                                        file_pattern.general_hhmmss_group, 
                                        file_pattern.general_version_group,
                                        file_pattern.general_revision_group,
                                        file_pattern.general_flare_class,
                                        file_pattern.general_extension_group],
                                        handle_missing_parts=True)
    
    if parts is None:  # No matches
        return None
    instrument, level, descriptor, year, month, day, thhmmss, file_version, file_revision, flare_class, extension = parts.values()

    version = file_version if file_version else '1'
    revision = file_revision if file_revision else '0'

    directory_path = os.path.dirname(fullpath)
    file_name = bn
    file_size = os.path.getsize(fullpath)
    if (maven_config.euv_flare_regex.match(bn) is not None or 
        maven_config.euv_regex.match(bn) is not None) and thhmmss!= '':
        hour = int(thhmmss[0:2])
        minute = int(thhmmss[2:4])
        second = 0
    elif thhmmss and thhmmss != '':
        hhmmss = int(thhmmss[1:])
        hour, mmss = divmod(hhmmss, 10000)
        minute, second = divmod(mmss, 100)
    else:
        hour, minute, second = 0, 0, 0
    # populate missing month and day only for flare catalogs
    if maven_config.euv_flare_catalog_regex.match(bn) is not None:
        month, day = 1, 1
    if '' in (year, month, day):
        timetag_utc = time_utilities.utc_now()
    else:
        timetag = datetime(int(year), int(month), int(day), hour, minute, second)
        timetag_utc = time_utilities.to_utc_tz(timetag)
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
            orbit_match = file_pattern.extract_parts([maven_config.orbit_regex],
                                                     tkns[1],
                                                     [maven_config.orbit_orbit_group])
            if orbit_match:
                orbit, = orbit_match.values()
            else:
                orbit = None
        if len(tkns) > 2:
            mode = tkns[2]
        if len(tkns) > 3:
            data_type = tkns[3]

    return ScFileMetadata(directory_path=directory_path,
                          file_name=file_name,
                          file_root=util_utilities.get_file_root_plus_extension(file_name, file_name),
                          file_size=file_size,
                          instrument=instrument,
                          level=level,
                          descriptor=descriptor,
                          timetag=timetag_utc,
                          absolute_version=util_utilities.get_absolute_version(int(version), int(revision)),
                          version=int(version),
                          revision=int(revision),
                          file_extension=extension,
                          plan=plan,
                          orbit=orbit,
                          mode=mode,
                          mod_date=util_utilities.get_mtime(fullpath),
                          data_type=data_type,
                          flare_class=flare_class)


def generate_metadata_for_ancillary_file(fullpath_list):
    '''Returns a list of the metadata embedded in the ancillary file name.

    Argument
        fullpath_list - A list of full paths of a quicklook, KP or
                        level 1, 2, or 3 files
    '''
    for f in fullpath_list:
        next_result = get_metadata_for_ancillary_file(f)
        if next_result:
            yield next_result


def get_metadata_for_ancillary_file(fullpath):
    '''Returns a list of the metadata embedded in the ancillary file name.

    Argument
        fullpath_list - A list of full paths of a quicklook, KP or
                        level 1, 2, or 3 files
    '''
    bn = os.path.basename(fullpath)
    m = file_pattern.extract_parts(regex_list=anc_config.ancillary_regex_list,
                                   string_to_parse=bn,
                                   parts=[maven_config.anc_base_group,
                                          maven_config.anc_product_group,
                                          anc_config.anc_doy_start_group,  # start groups
                                          anc_config.anc_yy_start_group,
                                          anc_config.anc_yyyy_start_group,
                                          file_pattern.general_yymmdd_group,
                                          file_pattern.general_yyyymmdd_group,
                                          anc_config.anc_orb_start,
                                          anc_config.anc_doy_end_group,  # end groups
                                          anc_config.anc_orb_end,
                                          file_pattern.general_hh_group,
                                          file_pattern.general_mm_group,
                                          file_pattern.general_yymmdd_group_end,
                                          file_pattern.general_extension_group,
                                          file_pattern.general_version_group],
                                   handle_missing_parts=True)

    if m is None:
        logger.debug("%s didn't match ancillary pattern", m)
        return None
    base, product, doy_start, yy_start, yyyy_start, yymmdd_start, yyyymmdd_start, orb_start, doy_end, orb_end, hh, mm, yymmdd_end, extension, version = m.values()

    # determine start
    start_date = None
    if yy_start:
        if not doy_start:
            logger.debug("Missing DOY.  Can't determine Ancillary Start Time for file %s" % bn)
            return None
        start_date = convert_ydoy_to_datetime(2000 + int(yy_start), int(doy_start))
    elif yyyy_start:
        if not doy_start:
            logger.debug("Missing DOY.  Can't determine Ancillary Start Time for file %s" % bn)
            return None
        start_date = convert_ydoy_to_datetime(int(yyyy_start), int(doy_start))
    elif yymmdd_start:
        start_date = time_utilities.make_utc_aware(yymmdd_start, '%y%m%d')
    elif yyyymmdd_start:
        start_date = time_utilities.make_utc_aware(yyyymmdd_start, '%Y%m%d')
    elif orb_start:
        start_date = get_orbit_perigee_time(int(orb_start))
    else:
        # TODO LOG
        pass

    if hh:
        start_date = start_date.replace(hour=int(hh))
    if mm:
        start_date = start_date.replace(minute=int(mm))
    # determine end
    end_date = None

    if doy_end:
        if yy_start:
            end_year = 2000 + int(yy_start)
            if doy_start and doy_start > doy_end:  # Rollover
                end_year += 1
            end_date = convert_ydoy_to_datetime(end_year, int(doy_end))
    elif yymmdd_end:
        end_date = time_utilities.make_utc_aware(yymmdd_end, '%y%m%d')
    elif orb_end:
        end_date = get_orbit_perigee_time(int(orb_end))
    else:
        # TODO LOG
        pass

    version = version if version and len(version) > 0 else None

    directory_path = os.path.dirname(fullpath)
    product = product or ""  # product cannot be null in the database
    file_name = bn
    file_size = os.path.getsize(fullpath)

    return AncFileMetadata(directory_path=directory_path,
                           file_name=file_name,
                           file_root=util_utilities.get_file_root_plus_extension(file_name, file_name),
                           base_name=base,
                           file_size=file_size,
                           product=product,
                           file_extension=extension,
                           start_date=start_date,
                           end_date=end_date,
                           mod_date=util_utilities.get_mtime(fullpath),
                           version=version)


def insert_l0_file_metadatum(metadata):
    '''Inserts the provided L0 metadata'''
    m = ScienceFilesMetadata(metadata.file_name,
                             metadata.file_root,
                             metadata.directory_path,
                             metadata.file_size,
                             metadata.instrument,
                             metadata.level,
                             metadata.timetag,
                             metadata.version,  # absolute_version == version for l0 files as there is no revision
                             metadata.version,
                             metadata.mod_date,
                             grouping=metadata.grouping)
    db_session.add(m)
    db_session.commit()


def upsert_l0_file_metadatum(metadata):
    '''Inserts or Updates the provided L0 metadata'''
    try:
        insert_l0_file_metadatum(metadata)
    except IntegrityError as e:
        db_session.rollback()
        db_session.commit()
        # try update
        sfm_id = db_session.query(ScienceFilesMetadata.id).filter(ScienceFilesMetadata.file_name == metadata.file_name).first()

        if sfm_id is None:
            raise e

        m = ScienceFilesMetadata(metadata.file_name,
                                 metadata.file_root,
                                 metadata.directory_path,
                                 metadata.file_size,
                                 metadata.instrument,
                                 metadata.level,
                                 metadata.timetag,
                                 metadata.version,  # absolute_version == version for l0 files as there is no revision
                                 metadata.version,
                                 metadata.mod_date,
                                 grouping=metadata.grouping)
        m.id = sfm_id[0]
        db_session.merge(m)
        db_session.commit()


def insert_l0_file_metadata(metadata_list):
    '''Inserts metadata about level 0 files into the database.'''
    for metadata in metadata_list:
        insert_l0_file_metadatum(metadata)


def upsert_l0_file_metadata(metadata_list,
                            exception_to_handle=None):
    '''Inserts or Updates metadata about level 0 files into the database.
    Arguments:
        exception_to_handle - The exception that should be handled.  If None, all exception will be propagated
    Returns
        failed_upserts - A list of (Exception,Metadata) for the files that failed to upsert in the event the
                         raised exception is handled'''
    failed_upserts = []
    for metadata in metadata_list:
        try:
            upsert_l0_file_metadatum(metadata)
        except(exception_to_handle) as e:
            logger.debug('Handling %s, %s was not indexed' % (e, metadata))
            failed_upserts.append((e, metadata))
    return failed_upserts


def insert_science_file_metadatum(metadata):
    '''Inserts metadata about quicklook, level 1, 2, or 3 files into the database.'''
    m = ScienceFilesMetadata(metadata.file_name,
                             metadata.file_root,
                             metadata.directory_path,
                             metadata.file_size,
                             metadata.instrument,
                             metadata.level,
                             metadata.timetag,
                             metadata.absolute_version,
                             metadata.version,
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
    console_logger.info(f"Added {metadata.file_name} to the metadata database")


def upsert_science_file_metadatum(metadata):
    '''Inserts or Updates metadata about quicklook, level 1, 2, or 3 files into the database'''
    try:
        insert_science_file_metadatum(metadata)
    except IntegrityError as e:
        db_session.rollback()
        db_session.commit()
        # try update
        sfm_id = db_session.query(ScienceFilesMetadata.id).filter(ScienceFilesMetadata.file_name == metadata.file_name).first()

        if sfm_id is None:
            raise e

        m = ScienceFilesMetadata(metadata.file_name,
                                 metadata.file_root,
                                 metadata.directory_path,
                                 metadata.file_size,
                                 metadata.instrument,
                                 metadata.level,
                                 metadata.timetag,
                                 metadata.absolute_version,
                                 metadata.version,
                                 revision=metadata.revision,
                                 mod_date=metadata.mod_date,
                                 descriptor=metadata.descriptor,
                                 file_extension=metadata.file_extension,
                                 plan=metadata.plan,
                                 orbit=metadata.orbit,
                                 mode=metadata.mode,
                                 data_type=metadata.data_type)
        m.id = sfm_id[0]
        db_session.merge(m)
        db_session.commit()
        console_logger.info(f"Updated {metadata.file_name} in the metadata database")


def insert_science_file_metadata(metadata_list):
    '''Inserts metadata about  quicklook, level 1, 2, or 3 files into the database.'''

    for metadata in metadata_list:
        insert_science_file_metadatum(metadata)


def upsert_science_file_metadata(metadata_list,
                                 exception_to_handle=None):
    '''Inserts or Updates metadata about  quicklook, level 1, 2, or 3 files into the database.
    Arguments:
        exception_to_handle - The exception that should be handled.  If None, all exception will be propagated
    Returns
        failed_upserts - A list of (Exception,Metadata) for the files that failed to upsert in the event the
                         raised exception is handled'''
    failed_upserts = []
    for metadata in metadata_list:
        try:
            upsert_science_file_metadatum(metadata)
        except(exception_to_handle) as e:
            logger.debug('Handling %s, %s was not indexed' % (e, metadata))
            failed_upserts.append((e, metadata))
    return failed_upserts


def insert_ancillary_file_metadatum(metadata):
    '''Inserts the provided metadata into the Ancillary files metadata table'''
    m = AncillaryFilesMetadata(
        metadata.file_name,
        metadata.file_root,
        metadata.base_name,
        metadata.directory_path,
        metadata.file_size,
        metadata.product,
        metadata.file_extension,
        metadata.mod_date,
        metadata.start_date,
        metadata.end_date,
        metadata.version)
    db_session.add(m)
    db_session.commit()


def upsert_ancillary_file_metadatum(metadata):
    '''Inserts or Updates the provided metadata into the Ancillary files metadata table'''
    afm_id = db_session.query(AncillaryFilesMetadata.id).filter(AncillaryFilesMetadata.file_name == metadata.file_name).filter(AncillaryFilesMetadata.directory_path == metadata.directory_path).first()

    if afm_id is None:
        insert_ancillary_file_metadatum(metadata)
        return

    m = AncillaryFilesMetadata(
        metadata.file_name,
        metadata.file_root,
        metadata.base_name,
        metadata.directory_path,
        metadata.file_size,
        metadata.product,
        metadata.file_extension,
        metadata.mod_date,
        metadata.start_date,
        metadata.end_date,
        metadata.version)
    m.id = afm_id[0]
    db_session.merge(m)
    db_session.commit()


def insert_ancillary_file_metadata(metadata_list):
    '''Inserts metadata about an ancillary file into the database.'''
    for metadata in metadata_list:
        insert_ancillary_file_metadatum(metadata)


def upsert_ancillary_file_metadata(metadata_list,
                                   exception_to_handle=None):
    '''Inserts or Updates metadata about an ancillary file into the database.
    Arguments:
        exception_to_handle - The exception that should be handled.  If None, all exception will be propagated
    Returns
        failed_upserts - A list of (Exception,Metadata) for the files that failed to upsert in the event the
                         raised exception is handled'''
    failed_upserts = []
    for metadata in metadata_list:
        try:
            upsert_ancillary_file_metadatum(metadata)
        except(exception_to_handle) as e:
            logger.debug('Handling %s, %s was not indexed' % (e, metadata))
            failed_upserts.append((e, metadata))
    return failed_upserts


def delete_l0_file_metadata(metadata_list):
    '''Deletes metadata about level 0 files into the database.'''
    for metadata in metadata_list:
        m = ScienceFilesMetadata(metadata.file_name,
                                 metadata.file_root,
                                 metadata.directory_path,
                                 metadata.file_size,
                                 metadata.instrument,
                                 metadata.level,
                                 metadata.timetag,
                                 metadata.version,  # absolute_version == version for l0 files as there is no revision
                                 metadata.version,
                                 metadata.mod_date,
                                 grouping=metadata.grouping)
        db_session.delete(m)
        db_session.commit()
#         logger.info("committed metadata {0}".format(metadata))


def delete_science_file_metadata(metadata_list):
    '''Deletes metadata about  quicklook, level 1, 2, or 3 files into the database.'''

    for metadata in metadata_list:
        m = ScienceFilesMetadata(metadata.file_name,
                                 metadata.file_root,
                                 metadata.directory_path,
                                 metadata.file_size,
                                 metadata.instrument,
                                 metadata.level,
                                 metadata.timetag,
                                 metadata.absolute_version,
                                 metadata.version,
                                 revision=metadata.revision,
                                 mod_date=metadata.mod_date,
                                 descriptor=metadata.descriptor,
                                 file_extension=metadata.file_extension,
                                 plan=metadata.plan,
                                 orbit=metadata.orbit,
                                 mode=metadata.mode,
                                 data_type=metadata.data_type)
        db_session.delete(m)
        db_session.commit()

        # logger.info("committed metadata %s", metadata)


def delete_science_file_metadata_from_filename(filename):
    try:
        db_session.query(ScienceFilesMetadata).filter(ScienceFilesMetadata.file_name == filename).delete()
        db_session.commit()
    except Exception:
        logger.exception('Unable to remove %s from the science database table', filename)
        return False
    return True


def delete_ancillary_file_metadata_from_filename(filename):
    try:
        db_session.query(AncillaryFilesMetadata).filter(AncillaryFilesMetadata.file_name == filename).delete()
        db_session.commit()
    except Exception:
        logger.exception('Unable to remove %s from the ancillary database table', filename)
        return False
    return True


def delete_ancillary_file_metadata(metadata_list):
    '''Deletes metadata about an ancillary file into the database.'''
    for metadata in metadata_list:
        m = AncillaryFilesMetadata(
            metadata.file_name,
            metadata.file_root,
            metadata.base_name,
            metadata.directory_path,
            metadata.file_size,
            metadata.product,
            metadata.file_extension,
            metadata.mod_date,
            metadata.start_date,
            metadata.end_date,
            metadata.version)
        db_session.delete(m)
        db_session.commit()
#         logger.info("committed metadata {0}".format(metadata))


def remove_lost_metadata(lost_list):
    '''Removes rows that don't have an associated SDC file from the database'''
    for metadata in lost_list:
        db_session.delete(metadata)
        db_session.commit()
        logger.debug('Removed metadata %s', metadata)
