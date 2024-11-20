from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, ForeignKey, BigInteger
from sqlalchemy.orm import relationship
from maven_database.database import Base, TableNameMixin
from maven_utilities import time_utilities
from sqlalchemy.sql.sqltypes import Numeric

# SQLAlchemy does not map BigInt to Int by default on the sqlite dialect.
from sqlalchemy.dialects import postgresql, sqlite

BigIntegerType = BigInteger()
BigIntegerType = BigIntegerType.with_variant(postgresql.BIGINT(), 'postgresql')
BigIntegerType = BigIntegerType.with_variant(sqlite.INTEGER(), 'sqlite')


class MavenDropboxMgrMove(Base):
    ''' Model for the maven_dropbox_mgr_moves table. '''

    __tablename__ = "maven_dropbox_mgr_moves"

    id = Column(Integer, primary_key=True)
    when_moved = Column(DateTime(timezone=True))
    src_filename = Column(String)
    dest_filename = Column(String)
    md5 = Column(String)
    file_size = Column(Integer)

    def __init__(self,
                 when_moved,
                 src_filename,
                 dest_filename,
                 md5,
                 file_size):
        '''Build maven dropbox manager move from its parts.'''
        assert when_moved.tzinfo is not None  # when moved must be timezone aware
        self.when_moved = when_moved
        self.src_filename = src_filename
        self.dest_filename = dest_filename
        self.md5 = md5
        self.file_size = file_size

    def __str__(self):
        '''Returns a string representation of this object.'''
        return "%s %s %s" % (str(self.when_moved),
                             self.src_filename,
                             self.dest_filename)

    __repr__ = __str__


class MavenLog(Base):
    ''' Model for the maven_logs table. '''

    __tablename__ = "maven_logs"

    id = Column(Integer, primary_key=True)
    logger = Column(String)
    level = Column(String)
    message = Column(String)
    created_at = Column(DateTime(timezone=True))

    def __init__(self, logger, level, message, created_at=None):
        '''Build maven log from its parts.'''
        self.logger = logger
        self.level = level
        self.message = message
        self.created_at = created_at or time_utilities.utc_now()

    def __str__(self):
        '''Returns a string representation of this object.'''
        return "%s:\t%s\t%s\t%s" % (str(self.created_at), self.logger, self.level, self.message)

    __repr__ = __str__


class ScienceFilesMetadata(Base):
    ''' Model for the science_files_metadata table. '''

    __tablename__ = "science_files_metadata"

    id = Column(Integer, primary_key=True)
    file_name = Column(String)
    file_root = Column(String, nullable=False)
    directory_path = Column(String)
    file_size = Column(Integer)
    instrument = Column(String)
    grouping = Column(String)
    level = Column(String)
    descriptor = Column(String)
    timetag = Column(DateTime(timezone=True))
    absolute_version = Column(Integer)
    version = Column(Integer)
    revision = Column(Integer)
    file_extension = Column(String)
    plan = Column(String)
    orbit = Column(String)
    mode = Column(String)
    data_type = Column(String)
    released = Column(Boolean, nullable=False)
    updated_last = Column(DateTime(timezone=True), nullable=True)
    mod_date = Column(DateTime(timezone=True))
    flare_class = Column(String)

    def __init__(self,
                 file_name,
                 file_root,
                 directory_path,
                 file_size,
                 instrument,
                 level,
                 timetag,
                 absolute_version,
                 version,
                 mod_date,
                 grouping=None,
                 descriptor=None,
                 revision=None,
                 file_extension=None,
                 plan=None,
                 orbit=None,
                 mode=None,
                 data_type=None,
                 released=False,
                 flare_class=None):
        '''Build data file metadata from its parts.'''
        self.file_name = file_name
        self.file_root = file_root
        self.directory_path = directory_path
        self.file_size = file_size
        self.instrument = instrument
        self.grouping = grouping
        self.level = level
        self.descriptor = descriptor
        self.timetag = timetag
        self.version = version
        self.mod_date = mod_date
        self.absolute_version = absolute_version
        self.revision = revision
        self.file_extension = file_extension
        self.plan = plan
        self.orbit = orbit
        self.mode = mode
        self.data_type = data_type
        self.released = released
        self.updated_last = None
        self.flare_class = flare_class

    def __str__(self):
        '''Returns a string representation of this object.'''
        return "%s %s %d" % (self.directory_path,
                             self.file_name,
                             self.file_size)

    __repr__ = __str__


class AncillaryFilesMetadata(Base):
    ''' Model for the ancillary_files_metadata table. '''

    __tablename__ = "ancillary_files_metadata"

    id = Column(Integer, primary_key=True)
    file_name = Column(String, nullable=False)
    file_root = Column(String, nullable=False)
    base_name = Column(String, nullable=False)
    directory_path = Column(String, nullable=False)
    file_size = Column(Integer, nullable=False)
    product = Column(String, nullable=False)
    file_extension = Column(String, nullable=False)
    start_date = Column(DateTime(timezone=True), nullable=True)
    end_date = Column(DateTime(timezone=True), nullable=True)
    version = Column(Integer, nullable=True)
    released = Column(Boolean, nullable=False)
    mod_date = Column(DateTime(timezone=True))

    def __init__(self,
                 file_name,
                 file_root,
                 base_name,
                 directory_path,
                 file_size,
                 product,
                 file_extension,
                 mod_date,
                 start_date=None,
                 end_date=None,
                 version=None,
                 released=False):
        '''Build data file metadata from its parts.'''
        self.file_name = file_name
        self.file_root = file_root
        self.base_name = base_name
        self.directory_path = directory_path
        self.file_size = file_size
        self.product = product
        self.mod_date = mod_date
        self.file_extension = file_extension if file_extension is not None else ''
        self.start_date = start_date
        self.end_date = end_date
        self.version = version
        self.released = released

    def __str__(self):
        '''Returns a string representation of this object.'''
        return "%s %s %d" % (self.directory_path,
                             self.file_name,
                             self.file_size)

    __repr__ = __str__


class KpFilesMetadata(TableNameMixin, Base):
    ''' Model for the kp_files_metadata table. '''

    __base_tablename__ = "kp_files_metadata"

    id = Column(Integer, primary_key=True)
    file_name = Column(String)
    directory_path = Column(String)
    file_size = Column(Integer)
    file_type = Column(String)
    timetag = Column(DateTime(timezone=True))
    version = Column(Integer)
    revision = Column(Integer)
    ingest_status = Column(String)

    in_situ_key_parameters = relationship(
        "InSituKeyParametersData", backref="kp_files_metadata")

    def __init__(self,
                 file_name,
                 directory_path,
                 file_size,
                 file_type,
                 timetag,
                 version,
                 revision,
                 ingest_status="STARTED"):
        '''Build kp file metadata from its parts.'''
        self.file_name = file_name
        self.directory_path = directory_path
        self.file_size = file_size
        self.file_type = file_type
        self.timetag = timetag
        self.version = version
        self.revision = revision
        self.ingest_status = ingest_status

    def __str__(self):
        '''Returns a string representation of this object.'''
        return "%s %s %d" % (self.directory_path,
                             self.file_name,
                             self.file_size)

    __repr__ = __str__


class MavenEventType(Base):
    '''Model for the maven_event_types table.'''

    __tablename__ = 'maven_event_types'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    is_discrete = Column(Boolean, nullable=False)
    description = Column(String, nullable=True)
    discussion = Column(String, nullable=True)
    mission_event_type_id = Column(Integer)

    maven_events = relationship("MavenEvent", backref="maven_event_type")

    def __init__(self,
                 name,
                 is_discrete,
                 description=None,
                 discussion=None,
                 mission_event_type_id=None):
        '''Build MavenEventType from its parts.'''
        self.name = name
        self.is_discrete = is_discrete
        self.description = description
        self.discussion = discussion
        self.mission_event_type_id = mission_event_type_id

    def __str__(self):
        '''Returns a string representation of this object.'''
        return '%s %s' % (self.name, self.is_discrete)

    __repr__ = __str__


class MavenEvent(Base):
    '''Model for the maven_events table.'''

    __tablename__ = 'maven_events'

    id = Column(Integer, primary_key=True)
    event_type_id = Column(
        Integer, ForeignKey("maven_event_types.id"), nullable=False)
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True), nullable=False)
    source = Column(String, nullable=False)
    description = Column(String, nullable=True)
    discussion = Column(String, nullable=True)
    modified_time = Column(DateTime(timezone=True), nullable=False)

    tags = relationship("MavenEventTag", backref="maven_event")

    def __init__(self,
                 event_type_id,
                 start_time,
                 end_time,
                 source,
                 modified_time,
                 description=None,
                 discussion=None
                 ):
        '''Build MavenEvent from its parts.'''
        self.event_type_id = event_type_id
        self.start_time = start_time
        self.end_time = end_time
        self.source = source
        self.description = description
        self.discussion = discussion
        self.modified_time = modified_time

    def __str__(self):
        '''Returns a string representation of this object.'''
        return '%s %s %s %s' % (self.event_type_id, self.start_time, self.end_time, self.source)

    __repr__ = __str__


class MavenEventTag(Base):
    '''Model for the maven_event_tags table.'''

    __tablename__ = 'maven_event_tags'

    id = Column(Integer, primary_key=True)
    event_id = Column(Integer, ForeignKey('maven_events.id'), nullable=False)
    tag = Column(String, nullable=False)

    def __init__(self,
                 event_id,
                 tag):
        '''Build MavenEventTag from its parts.'''
        self.event_id = event_id
        self.tag = tag

    def __str__(self):
        '''Returns a string representation of this object.'''
        return '%d %s' % (self.event_id, self.tag)

    __repr__ = __str__


class MavenOrbit(Base):
    '''Model for maven orbits'''

    __tablename__ = 'maven_orbit'

    orbit_number = Column(Integer, primary_key=True)
    orbit_periapse = Column(DateTime(timezone=True), nullable=False)
    orbit_apoapse = Column(DateTime(timezone=True), nullable=False)
    synched_at = Column(DateTime(timezone=True), nullable=False)
    synched_source = Column(String, nullable=False)

    def __init__(self,
                 orbit_number,
                 orbit_periapse,
                 orbit_apoapse,
                 synched_at=None,
                 synched_source=None):
        self.orbit_number = orbit_number
        self.orbit_periapse = orbit_periapse
        self.orbit_apoapse = orbit_apoapse
        self.synched_at = synched_at
        self.synched_source = synched_source

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                self.orbit_number == other.orbit_number and
                self.orbit_periapse == other.orbit_periapse and
                self.orbit_apoapse == other.orbit_apoapse and
                self.synched_source == other.synched_source)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        '''Returns a string representation of this object.'''
        return 'MavenOrbit \n\tOrbit number %s\n\tOrbit perigee %s\n\tOrbit apogee %s\n\tSynced At %s\n\tSynch Source %s' % (self.orbit_number,
                                                                                                                             self.orbit_periapse,
                                                                                                                             self.orbit_apoapse,
                                                                                                                             self.synched_at,
                                                                                                                             self.synched_source)

    __repr__ = __str__


class InSituKpQueryParameter(TableNameMixin, Base):
    '''Model for the in_situ_kp_query_parameters table.'''

    __base_tablename__ = 'in_situ_kp_query_parameters'

    id = Column(Integer, primary_key=True)
    query_parameter = Column(String, nullable=False, unique=True)
    instrument_name = Column(String, nullable=False)
    kp_column_name = Column(String, nullable=False)
    data_format = Column(String, nullable=False)
    units = Column(String, nullable=False)
    notes = Column(String, nullable=True)

    kp_data = relationship("InSituKeyParametersData", backref="kp_query_parameter")

    def __init__(self,
                 query_parameter,
                 instrument_name,
                 kp_column_name,
                 data_format,
                 units,
                 notes):
        '''Build InSituKpQueryParameter from its parts.'''
        self.query_parameter = query_parameter
        self.instrument_name = instrument_name
        self.kp_column_name = kp_column_name
        self.data_format = data_format
        self.units = units
        self.notes = notes

    def __str__(self):
        '''Returns a string representation of this object.'''
        return '%s => (%s, %s)' % (self.query_parameter, self.instrument_name, self.kp_column_name)

    __repr__ = __str__


class InSituKeyParametersData(TableNameMixin, Base):
    '''Model for the in_situ_key_parameters_data table.'''

    __base_tablename__ = 'in_situ_key_parameters_data'

    id = Column(BigIntegerType, primary_key=True, autoincrement=True)
    kp_files_metadata_id = Column(Integer, ForeignKey("{0}.id".format(KpFilesMetadata.__tablename__)))
    timetag = Column(DateTime(timezone=True), nullable=False)
    file_line_number = Column(Integer, nullable=False)
    data_value = Column(Float)
    in_situ_kp_query_parameters_id = Column(
        Integer, ForeignKey("{0}.id".format(InSituKpQueryParameter.__tablename__)))

    def __init__(self,
                 kp_files_metadata_id,
                 timetag,
                 file_line_number,
                 in_situ_kp_query_parameters_id,
                 data_value):
        '''Build InSituKeyParametersData from its parts.'''
        self.kp_files_metadata_id = kp_files_metadata_id
        self.timetag = timetag
        self.file_line_number = file_line_number
        self.in_situ_kp_query_parameters_id = in_situ_kp_query_parameters_id
        self.data_value = data_value

    def __str__(self):
        '''Returns a string representation of this object.'''
        return '%s %s' % (self.id, self.data_value)

    __repr__ = __str__


class PdsArchiveRecord(Base):
    '''Model for the pds_archive_record table.'''

    __tablename__ = 'pds_archive_record'

    id = Column(Integer, primary_key=True)
    generation_time = Column(DateTime(timezone=True), nullable=False)
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True), nullable=False)
    command_line = Column(String, nullable=False)
    configuration = Column(String, nullable=False)
    dry_run = Column(Boolean, nullable=False)
    result_directory = Column(String, nullable=True)
    bundle_file_name = Column(String, nullable=True)
    manifest_file_name = Column(String, nullable=True)
    checksum_file_name = Column(String, nullable=True)
    result_version = Column(Integer, nullable=True)
    generation_result = Column(String, nullable=True)
    pds_status = Column(String, nullable=True)
    notes = Column(String, nullable=True)

    def __init__(self,
                 generation_time,
                 start_time,
                 end_time,
                 command_line,
                 configuration,
                 dry_run,
                 result_directory,
                 bundle_file_name,
                 manifest_file_name,
                 checksum_file_name,
                 result_version,
                 generation_result,
                 pds_status=None,
                 notes=None
                 ):
        '''Build PdsArchiveRecord from its parts.'''
        self.generation_time = generation_time
        self.start_time = start_time
        self.end_time = end_time
        self.command_line = command_line
        self.configuration = configuration
        self.dry_run = dry_run
        self.result_directory = result_directory
        self.bundle_file_name = bundle_file_name
        self.manifest_file_name = manifest_file_name
        self.checksum_file_name = checksum_file_name
        self.result_version = result_version
        self.generation_result = generation_result
        self.pds_status = pds_status
        self.notes = notes

    def __str__(self):
        '''Returns a string representation of this object.'''
        return '''
Generation Time:%s
Archive Start:%s
Archive End:%s
Command Line: %s
Configuration: %s
Dry Run: %s
Results Directory: %s
Bundle file:%s
Manifest file:%s
Checksum file:%s
Result Version:%s
Generation Result:%s
PDS Status:%s
Notes:%s
''' % (self.generation_time,
            self.start_time,
            self.end_time,
            self.command_line,
            self.configuration,
            self.dry_run,
            self.result_directory,
            self.bundle_file_name,
            self.manifest_file_name,
            self.checksum_file_name,
            self.result_version,
            self.generation_result,
            self.pds_status,
            self.notes)

    __repr__ = __str__


class MavenStatus(Base):
    '''Model for the maven_status table.'''

    __tablename__ = 'maven_status'

    id = Column(Integer, primary_key=True)
    component_id = Column(String, nullable=False)
    event_id = Column(String, nullable=False)
    job_id = Column(Numeric, nullable=False)
    description = Column(String, nullable=True)
    timetag = Column(DateTime(timezone=True), nullable=False)
    summary = Column(String, nullable=True)

    def __init__(self,
                 component_id,
                 event_id,
                 job_id,
                 description,
                 timetag,
                 summary=None):
        '''Build MavenStatus from its parts.'''
        self.component_id = component_id
        self.event_id = event_id
        self.job_id = job_id
        self.description = description
        self.timetag = timetag
        self.summary = summary

    def __str__(self):
        '''Returns a string representation of this object.'''
        return 'Component [%s] Event [%s] Job ID [%s] summary [%s] description [%s] timetag [%s]' % (self.component_id,
                                                                                                     self.event_id,
                                                                                                     self.job_id,
                                                                                                     self.summary,
                                                                                                     self.description,
                                                                                                     self.timetag)

    __repr__ = __str__
