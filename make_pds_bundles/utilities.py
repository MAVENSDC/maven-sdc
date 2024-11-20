''' Utility methods used to aid in PDS4 archiving
Created on Mar 9, 2015

@author: bstaley
'''
import datetime
import operator
import logging
import os
import csv
from io import StringIO
from sqlalchemy import and_, or_

from maven_database.models import ScienceFilesMetadata, MavenEvent, AncillaryFilesMetadata
from maven_ops_database.models import OpsMissionEvent
from maven_database import db_session
from maven_ops_database.database import db_session as ops_db_session
from maven_utilities import time_utilities

logger = logging.getLogger('maven.make_pds_bundles.make_pds_bundles.utilities.log')


def query_for_ancillary_files(product_list=None,
                              extension_list=None,
                              from_dt=datetime.datetime.min,
                              to_dt=datetime.datetime.max):
    '''Method used to query for AncillaryFilesMetadata for ancillary files of interest
    Arguments:
        product_list - The list of products to filter
        extension_list - The list of extensions to filter
        from_dt - The from time to filter
        to_dt - The to time to filter
    Returns
        The list of AncillaryFilesMetadata that meet the filter criteria
    '''
    # build query..
    query = db_session.query(AncillaryFilesMetadata)

    # Add time filter
    query = query.filter(and_(
                         AncillaryFilesMetadata.start_date <= to_dt,
                         AncillaryFilesMetadata.end_date > from_dt))

    product_list = product_list or []
    extension_list = extension_list or []
    
    product_filter = []
    for product in product_list:
        product_filter.append(operator.eq(AncillaryFilesMetadata.product, product))
        logger.debug("adding %s to AncillaryFileMetadata.product filter", product_filter)
    query = query.filter(or_(*product_filter))

    extension_filter = []
    for ext in extension_list:
        extension_filter.append(operator.eq(AncillaryFilesMetadata.file_extension, ext))
        logger.debug("adding %s to AncillaryFileMetadata.product filter", extension_filter)
    query = query.filter(or_(*extension_filter))

    return query.all()


def get_all_ancillary_files(product_list=None,
                            extension_list=None,
                            from_dt=datetime.datetime.min,
                            to_dt=datetime.datetime.max):
    '''Method used to query for AncillaryFilesMetadata for ancillary files of interest
    Arguments:
        product_list - The list of products to filter
        extension_list - The list of extensions to filter
        from_dt - The from time to filter
        to_dt - The to time to filter
    Returns
         list of fully qualified file names that meet the criteria
    '''
    # Correction for W0102 dangerous default value %s as argument
    product_list = product_list or []
    extension_list = extension_list or []

    results = []
    query_results = query_for_ancillary_files(product_list=product_list,
                                              extension_list=extension_list,
                                              from_dt=from_dt,
                                              to_dt=to_dt)

    for next_result in query_results:
        results.append('/'.join([next_result.directory_path, next_result.file_name]))

    return results


def query_for_science_files(instrument_list=None,
                            grouping_list=None,
                            plan_list=None,
                            level_list=None,
                            version=None,
                            revision=None,
                            extension_list=None,
                            description_list=None,
                            file_name=None,
                            from_dt=datetime.datetime.min,
                            to_dt=datetime.datetime.max,
                            latest=False,
                            stream_results=False
                            ):
    ''' Method used to query the science_files_metatdata table for science files of interest
    Arguments:
        instrument_list - The list of acceptable instruments to filter
        grouping_list - The list of groupings to filter
        plan_list - The list of plans to filter
        level_list - The list of levels to filter
        version - The version to filter
        revision - The revision to filter
        extension_list - The list of extensions to filter
        description_list - The list of acceptable descriptions (supports % wildcards)
        from_dt - The from time to filter
        to_dt - The to time to filter
        latest - Retrieve only the latest (in terms of version/revision) entries
        stream_results - Stream results from database
    Returns
        The list of ScienceFilesMetadata that meet the filter criteria
    '''
    # Correction for W0102 dangerous default value %s as argument
    # If list does not contain any element, default set to empty []
    instrument_list = instrument_list or []
    grouping_list = grouping_list or []
    plan_list = plan_list or []
    level_list = level_list or []
    extension_list = extension_list or []
    description_list = description_list or []

    # build query..
    query = db_session.query(ScienceFilesMetadata)

    # Add time filter
    query = query.filter(and_(
                         ScienceFilesMetadata.timetag >= from_dt,
                         ScienceFilesMetadata.timetag < to_dt))

    if version is not None:
        logger.debug("Adding ScienceFilesMetadata.version query for %s version", version)
        query = query.filter(ScienceFilesMetadata.version == version)

    if revision is not None:
        logger.debug("Adding ScienceFilesMetadata.revision query for %s revision", revision)
        query = query.filter(ScienceFilesMetadata.revision == revision)

    instrument_filter = []
    for instrument in instrument_list:
        instrument_filter.append(operator.eq(ScienceFilesMetadata.instrument, instrument))
        logger.debug("Adding ScienceFilesMetadata.instrument query for %s", instrument)
    query = query.filter(or_(*instrument_filter))

    grouping_filter = []
    for group in grouping_list:
        grouping_filter.append(operator.eq(ScienceFilesMetadata.grouping, group))
        logger.debug("Adding ScienceFilesMetadata.grouping query for %s", group)
    query = query.filter(or_(*grouping_filter))

    plan_filter = []
    for plan in plan_list:
        plan_filter.append(operator.eq(ScienceFilesMetadata.plan, plan))
        logger.debug("Adding ScienceFilesMetadata.plan query for %s", plan)
    query = query.filter(or_(*plan_filter))

    level_filter = []
    for level in level_list:
        level_filter.append(operator.eq(ScienceFilesMetadata.level, level))
    query = query.filter(or_(*level_filter))

    extension_filter = []
    for ext in extension_list:
        extension_filter.append(operator.eq(ScienceFilesMetadata.file_extension, ext))
        logger.debug("Adding ScienceFilesMetadata.file_extension query for %s", ext)
    query = query.filter(or_(*extension_filter))

    description_filter = []
    for desc in description_list:
        if '%' in desc:
            description_filter.append(ScienceFilesMetadata.descriptor.like(desc))
        else:
            description_filter.append(operator.eq(ScienceFilesMetadata.descriptor, desc))
        logger.debug("Adding ScienceFilesMetadata.file_extension query for %s", desc)
    query = query.filter(or_(*description_filter))

    if file_name:
        if '%' in file_name:
            query = query.filter(ScienceFilesMetadata.file_name.like(file_name))
        else:
            query = query.filter(ScienceFilesMetadata.file_name == file_name)
        logger.debug("Adding ScienceFilesMetadata.file_name query for %s", file_name)

    if latest:
        if 'metadata' in plan_list:
            query = query.order_by(ScienceFilesMetadata.descriptor.desc(), ScienceFilesMetadata.level.desc(),
                                   ScienceFilesMetadata.file_extension.desc(),
                                   ScienceFilesMetadata.absolute_version.desc(), ScienceFilesMetadata.timetag.desc()).distinct(
                ScienceFilesMetadata.descriptor, ScienceFilesMetadata.level, ScienceFilesMetadata.file_extension)
        else:
            query = query.order_by(ScienceFilesMetadata.file_root.desc(), ScienceFilesMetadata.absolute_version.desc()).distinct(ScienceFilesMetadata.file_root)

    if stream_results:
        return query.yield_per(20)
    # else plain old exhaust and return
    return query.all()


def get_all_science_files(instrument_list=None,
                          grouping_list=None,
                          plan_list=None,
                          level_list=None,
                          version=None,
                          revision=None,
                          extension_list=None,
                          from_dt=datetime.datetime.min,
                          to_dt=datetime.datetime.max):
    ''' Method used to query the science_files_metatdata table for science files of interest
    Arguments:
        instrument_list - The list of instruments to filter
        grouping_list - The list of groupings to filter
        plan_list - The list of plans to filter
        level_list - The list of levels to filter
        version - The version to filter
        revision - The revision to filter
        extension_list - The list of extensions to filter
        from_dt - The from time to filter
        to_dt - The to time to filter
    Returns
        The list of fully qualified file names that meet the criteria
    '''
    # Correction for W0102 dangerous default value %s as argument
    instrument_list = instrument_list or []
    grouping_list = grouping_list or []
    plan_list = plan_list or []
    level_list = level_list or []
    extension_list = extension_list or []

    results = []
    query_results = query_for_science_files(instrument_list=instrument_list,
                                            grouping_list=grouping_list,
                                            plan_list=plan_list,
                                            level_list=level_list,
                                            version=version,
                                            revision=revision,
                                            extension_list=extension_list,
                                            from_dt=from_dt,
                                            to_dt=to_dt)
    
    for next_result in query_results:
        
        
        results.append('/'.join([next_result.directory_path, next_result.file_name]))

    return results


def get_latest_science_metadata(instrument_list=None,
                                grouping_list=None,
                                plan_list=None,
                                level_list=None,
                                version=None,
                                revision=None,
                                extension_list=None,
                                from_dt=datetime.datetime.min,
                                to_dt=datetime.datetime.max):
    ''' Method used to query the science_files_metatdata table for science files of interest
    and return only latest version based on version/revision.  Version 0 files are never returned.
    Arguments:
        instrument_list - The list of instruments to filter
        grouping_list - The list of groupings to filter
        plan_list - The list of plans to filter
        level_list - The list of levels to filter
        version - The version to filter
        revision - The revision to filter
        extension_list - The list of extensions to filter
        from_dt - The from time to filter
        to_dt - The to time to filter
    Returns
        The list of fully qualified file names that meet the criteria and are considered
        the latest version based on version/revision
    '''
    # Correction for W0102 dangerous default value %s as argument
    instrument_list = instrument_list or []
    grouping_list = grouping_list or []
    plan_list = plan_list or []
    level_list = level_list or []
    extension_list = extension_list or []

    results = []
    query_results = query_for_science_files(instrument_list=instrument_list,
                                            grouping_list=grouping_list,
                                            level_list=level_list,
                                            plan_list=plan_list,
                                            version=version,
                                            revision=revision,
                                            extension_list=extension_list,
                                            from_dt=from_dt,
                                            to_dt=to_dt)
    # Arrange in Instrument:Level:Plan:Ext:Timetag->[(Version,Revision),(metadata)]
    ordered_results = {}
    running_max_metadata = []
    for next_result in query_results:
        ordered_results.setdefault(next_result.instrument, {}).setdefault(next_result.level, {}).setdefault(next_result.descriptor, {}).setdefault(next_result.file_extension, {}).setdefault(next_result.timetag, []).append(((next_result.absolute_version), next_result))

    for instrument in ordered_results:
        for level in ordered_results[instrument]:
            for descriptor in ordered_results[instrument][level]:
                for ext in ordered_results[instrument][level][descriptor]:
                    for time in ordered_results[instrument][level][descriptor][ext]:
                        logger.info('Latest Science Files Result for:\n\t Instrument:%s\n\t Level:%s\n\t Descriptor:%s\n\t Ext:%s\n\t Time:%s\n\t Result:%s',
                                    instrument, level, descriptor, ext, time, ordered_results[instrument][level][descriptor][ext][time])
                        _, max_metadata = max(ordered_results[instrument][level][descriptor][ext][time])
                        running_max_metadata.append(max_metadata)
    # Filter on versions > 0
    for next_max_metadata in [m for m in running_max_metadata if m.version > 0]:
        results.append(next_max_metadata)

    return results


def get_latest_science_files(instrument_list=None,
                             grouping_list=None,
                             plan_list=None,
                             level_list=None,
                             version=None,
                             revision=None,
                             extension_list=None,
                             from_dt=datetime.datetime.min,
                             to_dt=datetime.datetime.max):
    ''' Method used to query the science_files_metatdata table for science files of interest
    and return only latest version based on version/revision.  Version 0 files are never returned.
    Arguments:
        instrument_list - The list of instruments to filter
        grouping_list - The list of groupings to filter
        plan_list - The list of plans to filter
        level_list - The list of levels to filter
        version - The version to filter
        revision - The revision to filter
        extension_list - The list of extensions to filter
        from_dt - The from time to filter
        to_dt - The to time to filter
    Returns
        The list of fully qualified file names that meet the criteria and are considered
        the latest version based on version/revision
    '''
    # Correction for W0102 dangerous default value %s as argument
    instrument_list = instrument_list or []
    grouping_list = grouping_list or []
    plan_list = plan_list or []
    level_list = level_list or []
    extension_list = extension_list or []

    results = []
    latest_metadata = get_latest_science_metadata(instrument_list=instrument_list,
                                                  grouping_list=grouping_list,
                                                  level_list=level_list,
                                                  plan_list=plan_list,
                                                  version=version,
                                                  revision=revision,
                                                  extension_list=extension_list,
                                                  from_dt=from_dt,
                                                  to_dt=to_dt)

    for m in latest_metadata:
        results.append(os.path.join(m.directory_path, m.file_name))

    return results


def query_for_events(from_dt, to_dt):
    '''Method used to query for MAVEN SDC events that overlap from_dt and to_dt ordered by start_time ascending
    Arguments:
        from_dt - The from time to filter
        to_dt - The to time to filter
    Returns:
        A list of MavenEvents
    '''
    query = db_session.query(MavenEvent)

    logger.debug("querying for events from %s to %s", MavenEvent.start_time, MavenEvent.end_time)

    # Add time filter
    query = query.filter(
        MavenEvent.start_time <= to_dt,
        MavenEvent.end_time > from_dt).order_by(MavenEvent.start_time)
    return query.all()


def query_for_ops_events(from_dt, to_dt):
    '''Method used to query for MAVEN OPS events that overlap from_dt and to_dt ordered by start_time ascending
    Arguments:
        from_dt - The from time to filter
        to_dt - The to time to filter
    Returns:
        A list of MavenEvents
    '''
    query = ops_db_session.query(OpsMissionEvent)

    logger.debug("querying for ops events from %s to %s", OpsMissionEvent.starttime, OpsMissionEvent.endtime)

    # Add time filter
    query = query.filter(
        OpsMissionEvent.starttime <= to_dt,
        OpsMissionEvent.endtime > from_dt).order_by(OpsMissionEvent.starttime)
    return query.all()


def generate_ops_events_csv(ops_events):
    '''Returns a StringIO with contents in csv format.
    Arguments
        ops_events - a list of OpsMissionEvent
    Returns
        StringIO containing the generated csv'''
    csv_output = StringIO()
    csv_writer = csv.writer(csv_output, delimiter=',')

    events = []

    # Write header
    csv_writer.writerow([
        'id',
        'event_type_id',
        'start_time',
        'end_time',
        'source',
        'description',
        'discussion', ])

    for event in ops_events:
        stst = time_utilities.to_utc_tz(event.starttime).isoformat()
        etst = time_utilities.to_utc_tz(event.endtime).isoformat()

        events.append((event.eventid,
                       event.eventtypeid,
                       stst,
                       etst,
                       event.source,
                       event.description,
                       event.discussion))

    events = sorted(events, key=lambda x: x[2])

    for event in events:
        csv_writer.writerow(event)

    return csv_output


def generate_sdc_events_csv(sdc_events):
    '''Returns a StringIO with contents in csv format.
    Arguments
        sdc_events - a list of MaventEvent
    Returns
        StringIO containing the generated csv'''
    csv_output = StringIO()
    csv_writer = csv.writer(csv_output, delimiter=',')

    events = []

    # Write header
    csv_writer.writerow([
        'id',
        'event_type_id',
        'start_time',
        'end_time',
        'source',
        'description',
        'discussion',
        'modified_time', ])

    for event in sdc_events:
        stst = time_utilities.to_utc_tz(event.start_time).isoformat()
        etst = time_utilities.to_utc_tz(event.end_time).isoformat()
        modst = time_utilities.to_utc_tz(event.modified_time).isoformat()

        events.append((event.id,
                       event.event_type_id,
                       stst,
                       etst,
                       event.source,
                       event.description,
                       event.discussion,
                       modst))

    events = sorted(events, key=lambda x: x[2])

    for event in events:
        csv_writer.writerow(event)

    return csv_output
