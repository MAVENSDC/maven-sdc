'''
Created on Mar 10, 2015

@author: bstaley
         tbussell
'''
import os
import pytz
import random
from datetime import datetime, timedelta
from maven_utilities import constants, time_utilities, utilities as util_utilities
os.environ[constants.python_env] = 'testing'
from maven_database import db_session
from maven_orbit import orbit_db_session
from maven_database.models import MavenEventType, MavenEvent, MavenEventTag, ScienceFilesMetadata, \
    MavenStatus
from maven_database.models import AncillaryFilesMetadata, InSituKpQueryParameter, KpFilesMetadata, InSituKeyParametersData
from maven_database.models import MavenLog, MavenDropboxMgrMove, PdsArchiveRecord, MavenOrbit
from maven_ops_database.models import OpsMissionEvent, OpsMissionEventOrbitNumber, OpsMissionEventType
from maven_ops_database.database import db_session as ops_db_session


def create_maven_events(start_time,
                        num_events,
                        delta_seconds,
                        type_name='Test Event',
                        source_name='Test Source',
                        event_duration=30,
                        create_tags=True):
    '''Creates events in the database for testing and returns a tuple containing a MavenEventType,
     a list of MavenEvents, a list of MavenEventTags, and a list of MavenEventOrbitNumbers'''
    check = MavenEventType.query.filter(MavenEventType.name == type_name).all()

    assert(len(check) == 0)

    event_type = MavenEventType(type_name, False)
    db_session.add(event_type)
    db_session.commit()
    test_time = start_time
    event_list = []
    tag_list = []
    orbit_number_list = []
    for _ in range(num_events):
        end_time = test_time + timedelta(seconds=event_duration)
        e = MavenEvent(event_type.id,
                       test_time,
                       end_time,
                       source_name,
                       test_time,
                       'test description')
        db_session.add(e)
        db_session.commit()
        event_list.append(e)

        if create_tags:
            t = MavenEventTag(e.id, 'test tag')
            db_session.add(t)
            db_session.commit()
            tag_list.append(t)

        test_time += timedelta(seconds=delta_seconds)

    return (event_type, event_list, tag_list, orbit_number_list)


def create_ancillary_metadata(base='sci_anc_', product_list=['test'], year_list=['13'], mod_date=None, start_doy=1, end_doy=2, ext='drf', root_dir='/tmp', version=None):
    file_list = []
    mod_date = mod_date or time_utilities.utc_now()
    for product in product_list:
        for year in year_list:
            running_doy = start_doy
            while running_doy < end_doy:
                file_name = '%s%s%s_%s_%s.%s' % (base,
                                                 product,
                                                 year,
                                                 "{:0>3d}".format(start_doy),
                                                 "{:0>3d}".format(end_doy),
                                                 ext
                                                 )
                start = datetime(int(year) + 2000, 1, 1) + \
                    timedelta(start_doy - 1)
                start = start.replace(tzinfo=pytz.UTC)
                end = datetime(int(year) + 2000, 1, 1) + timedelta(end_doy - 1)
                end = end.replace(tzinfo=pytz.UTC)
                next_anc = AncillaryFilesMetadata(file_name, util_utilities.get_file_root_plus_extension(
                    file_name, file_name), base, root_dir, 0, product, ext, mod_date, start, end, version)
                db_session.add(next_anc)
                db_session.commit()

                running_doy = running_doy + 1
                file_list.append('/'.join([root_dir, file_name]))

    return file_list


def insert_ancillary_file_metadata(file_name=None, base_name='sci_anc_bn', directory_path='/',
                                   file_size=42, product='bn', file_extension='drf',
                                   mod_date=None,
                                   start_date=None,
                                   end_date=None,
                                   version=None,
                                   released=False):
    '''Inserts fake metadata about an ancillary file into the database.'''
    mod_date = mod_date or time_utilities.utc_now()
    if file_name is None:
        file_name = 'sci_anc_%s13_123_123_.%s' % (base_name, file_extension)

    m = AncillaryFilesMetadata(file_name,
                               util_utilities.get_file_root_plus_extension(
                                   file_name, file_name),
                               base_name,
                               directory_path,
                               file_size,
                               product,
                               file_extension,
                               mod_date,
                               start_date,
                               end_date,
                               version,
                               released)

    db_session.add(m)
    db_session.commit()


def insert_science_files_metadata(file_name=None, file_root=None, dir_path="/", file_size=42, instrument="kp", level="l1", timetag=time_utilities.utc_now(),
                                  absolute_version=None, version=1, revision=None, mod_date=None, grouping="all", descriptor=None, extension=None, plan=None, orbit=None, mode=None,
                                  data_type=None, released=False, flare_class=None):
    '''
    Inserts fake science files metadata into the database.
    <file_name> is the desired filename. If no file_name is provided the filename of the data will be of the form:
                mvn_<instrument>_<grouping>_<level>_<timetag>_v<version>.dat where the timetag is converted to YYYYMMDD.
                NOTE: Because the timetag is truncated after DD, you cannot create multiple files using only the default file_name
                and different timetags from the same day, ie a difference in seconds is not sufficient.
    <file_root> The file root which consists of the file name minus any version/revision information
    <dir_path> is a string indicating the directory path to the file. Its default is "/".
    <file_size> is an integer indicating the size of the file. Its default is 42.
    <instrument> is a string indicating the instrument type. Its default is "inst".
    <level> is a string indicating the desired level. Its default is "l1".
    <timetag> is an appropriately formatted datetime (UTC) object. Its default is utcnow().
    <absolute_version> is an integer indicating the absolute version number of the metadata. Its default is None (will be calculated)
    <version> is an integer indicating the version number of the metadata. Its default is 1
    <revision> is an integer indicating the revision number of the metadata. Its default is None.
    <grouping> is a string indicating the grouping of the metadata. I's default is "group".
    <descriptor> is a string indicating the descriptor of the metadata. Its default is None.
    <extension> is a string indicating the file extension of the metadata. Its default is None.
    <plan> is a string indicating the plan of the metadata. Its default is None.
    <orbit> is a string indicating the orbit of the metadata. Its default is None.
    <mode> is a string indicating the mode of the metadata. Its default is None.
    <data_type> is a string indicating the data type of the metadata. Its default is None.
    '''
    mod_date = mod_date or time_utilities.utc_now()
    if file_name is None:
        file_name = "mvn_%s_%s_%s_%04d%02d%02dT%02d%02d%02d_v%02d.dat" % (instrument,
                                                                          grouping,
                                                                          level,
                                                                          timetag.year,
                                                                          timetag.month,
                                                                          timetag.day,
                                                                          timetag.hour,
                                                                          timetag.minute,
                                                                          timetag.second,
                                                                          version)

    if file_root is None:
        file_root = util_utilities.get_file_root_plus_extension(
            file_name, file_name)

    if absolute_version is None:
        absolute_version = util_utilities.get_absolute_version(
            version, revision)

    m = ScienceFilesMetadata(file_name, file_root, dir_path, file_size, instrument, level, timetag, absolute_version, version,
                             mod_date, grouping, descriptor, revision, extension, plan, orbit, mode, data_type, released, flare_class)
    

    db_session.add(m)
    db_session.commit()

    return m


def insert_kp_meta_data(file_name="mvn_kp_insitu_20150419_v01_r01", directory_path='/', file_size=42, file_type="tab",
                        timetag=time_utilities.utc_now(), version=1, revision=1, ingest_status="STARTED"):
    '''
    Inserts fake KP meta data into database and returns a KpFilesMetadata object.
    <file_name> is the desired filename. file_name should be properly formatter string,
                eg  "mvn_kp_insitu_YYYYMMDD_v01_r01". Its default argument is "mvn_kp_insitu_20150419_v01_r01".
    <directory_path> is a string indicating the directory path to the file. Its default is "/".
    <file_size> is an integer indicating the size of the file. Its default is 42.
    <file_type> is a string indicating the desired file type. Its default is 'tab'.
    <timetag> is an appropriately formatted datetime (UTC) object. Its default is utcnow().
    <version> is an integer indicating the version number of the metadata. Its default is 1.
    <revision> is an integer indicating the revision number of the metadata. Its default is 1.
    '''

    metadata = KpFilesMetadata(file_name=file_name,
                               directory_path=directory_path,
                               file_size=file_size,
                               file_type=file_type,
                               timetag=timetag,
                               version=version,
                               revision=revision,
                               ingest_status=ingest_status)
    db_session.add(metadata)
    db_session.commit()
    return metadata


def insert_in_situ_kp_query_parameter(query_parameter="timestamp", instrument_name="inst", column="col1",
                                      data_format="format", units="units", notes="notes"):
    '''
    Inserts fake in situ kp query parameter data into the database and returns an InSituKpQueryParameter object.
    <query_parameter> is a string indicating the desired query parameter. Its default is "timestamp".
    <instrument_name> is a string indicating the desired instrument name. Its default is "inst".
    <column> is a string indicating the desired column name. Its default is "col1".
    <data_format> is a string indicating the desired data format. Its default is "format".
    <units> is a string indicating the desired units. Its default is "units".
    <notes> is a string indicating the desired notes. Its default is "notes".
    '''

    new_qp = InSituKpQueryParameter(query_parameter=query_parameter,
                                    instrument_name=instrument_name,
                                    kp_column_name=column,
                                    data_format=data_format,
                                    units=units,
                                    notes=notes)

    db_session.add(new_qp)
    db_session.commit()
    return new_qp


def insert_in_situ_key_parameters_data(metadata, query_parameters,
                                       timetag=time_utilities.utc_now(),
                                       file_line_number=1, data_value=1.5):
    '''
    Inserts fake in situ key parameters data into the database.
    <metadata> is a KpFilesMetadata object.
    <query_parameters_id> is the integer id returned by insert_in_situ_kp_query_parameter().
    <timetag> is an appropriately formatted datetime (UTC) object. Its default is utcnow().
    <file_line_number> is an integer indicating the desired file line number. Its default is 1.
    <data_value> is float indicating the desired data value. Its default is 1.5.
    '''

    kpd = InSituKeyParametersData(kp_files_metadata_id=metadata.id,
                                  timetag=timetag,
                                  file_line_number=file_line_number,
                                  in_situ_kp_query_parameters_id=query_parameters.id,
                                  data_value=data_value)
    db_session.add(kpd)
    db_session.commit()


def insert_in_situ_key_parameters_data_range(metadata, query_parameters_ids,
                                             from_dt, to_dt, msecs_step=1,
                                             data_vals=[1], data_variances=[0]):
    '''
    Inserts fake in situ key parameters data over a time range.
    <metadata> is a KpFilesMetadata object.
    <query_parameters_id> is the integer id returned by insert_in_situ_kp_query_parameter().
    <from_dt> is an appropriately formatted datetime (UTC) object that indicates the
              desired beginning of the time range.
    <to_dt> is an appropriately formatted datetime (UTC) object that indicates the
              desired end of the time range.
    <msecs_step> is an integer indicating the desired step size in milliseconds. Its default is 1.
    <data_vals> is a list of floats indicating the desired data values. Its default is [1].
    <data_variances> is a list of floats indicating the desired data variances. Its default is [0].
    '''

    start_stop_diff = to_dt - from_dt

    # Make range inclusive to include end
    for i in range(0, int(start_stop_diff.total_seconds() * 1000) + msecs_step, msecs_step):
        secs_delta = timedelta(milliseconds=i)
        test_time = from_dt + secs_delta
        j = 0
        for qp in query_parameters_ids:
            kpd = InSituKeyParametersData(metadata.id,
                                          test_time,
                                          i,
                                          qp.id,
                                          data_vals[j if j < len(data_vals) else -1] + 
                                          (data_variances[j if j < len(data_variances) else -1] * 
                                           random.random()))
            j += 1
            db_session.add(kpd)
            db_session.commit()


def insert_ops_events(start_time,
                      num_events,
                      delta_seconds,
                      type_name='Test Event',
                      source_name='Test Source',
                      event_duration=30,
                      insert_orbit_number=None):
    event_list = []
    # does type exist?
    ops_type = OpsMissionEventType.query.filter(
        OpsMissionEventType.name == type_name).first()
    test_time = start_time
    if not ops_type:
        next_type_id = OpsMissionEventType.query.first(
        ).eventtypeid if OpsMissionEventType.query.count() > 1 else 1
        ops_type = OpsMissionEventType(eventtypeid=next_type_id,
                                       name=type_name,
                                       label='test label',
                                       isdiscrete='T',
                                       description='test description',
                                       discussion='test discussion')
        ops_db_session.add(ops_type)
        ops_db_session.commit()

    next_event_id = OpsMissionEvent.query.first(
    ).eventid if OpsMissionEvent.query.count() > 1 else 1

    for i in range(num_events):
        end_time = test_time + timedelta(seconds=event_duration)

        next_event = OpsMissionEvent(eventid=next_event_id,
                                     eventtypeid=ops_type.eventtypeid,
                                     starttime=test_time,
                                     endtime=end_time,
                                     source=source_name,
                                     description='test description %s' % i,
                                     discussion='test discussion %s' % i)
        ops_db_session.add(next_event)
        ops_db_session.commit()

        event_list.append(next_event)

        if insert_orbit_number:
            next_on = OpsMissionEventOrbitNumber(
                event_id=next_event.eventid, orbit_number=insert_orbit_number)
            ops_db_session.add(next_on)
            ops_db_session.commit()
            insert_orbit_number += 1

        next_event_id += 1
        test_time += timedelta(seconds=delta_seconds)

    return event_list


def insert_orbit(number,
                 periapse,
                 apoapse,
                 synced_at=None,
                 synched_source='test orbit source'):
    '''Method used to insert an entry into the maven_orbit table
    Arguments:
        number = The orbit number
        periapse - The periapse time
        apoapse - The apoapse time
        synched_at - The time in which the data was synched
        synched_source - The source of the data synch
    '''
    synced_at = synced_at if synced_at else time_utilities.utc_now()

    orbit = MavenOrbit(orbit_number=number,
                       orbit_periapse=periapse,
                       orbit_apoapse=apoapse,
                       synched_at=synced_at,
                       synched_source=synched_source)
    orbit_db_session.add(orbit)
    orbit_db_session.commit()


def delete_data(*to_delete):
    ''' Deletes selected or all data from the database.
    <to_delete> is any number of objects indicating the table to be cleared. If no objects are supplied
                all tables in the master_order list will be cleared.
    '''
    master_order = [MavenOrbit, InSituKeyParametersData, InSituKpQueryParameter, KpFilesMetadata, ScienceFilesMetadata,
                    MavenEventTag, MavenEvent, MavenEventType, AncillaryFilesMetadata,
                    MavenDropboxMgrMove, MavenLog, PdsArchiveRecord, MavenStatus]

    ordered_to_delete = sorted(to_delete, key=master_order.index)

    if len(to_delete) == 0:
        ordered_to_delete = master_order

    for table in ordered_to_delete:
        table.query.delete()
    db_session.commit()
    orbit_db_session.commit()


def delete_ops_data(*to_delete):
    ''' Deletes selected or all data from the database.
    <to_delete> is any number of objects indicating the table to be cleared. If no objects are supplied
                all tables in the master_order list will be cleared.
    '''
    master_order = [
        OpsMissionEventOrbitNumber, OpsMissionEvent, OpsMissionEventType]

    ordered_to_delete = sorted(to_delete, key=master_order.index)

    if len(to_delete) == 0:
        ordered_to_delete = master_order

    for table in ordered_to_delete:
        table.query.delete()
        ops_db_session.commit()
