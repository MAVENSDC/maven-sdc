#! /usr/bin/env python
#
# Kim Kokkonen 2014-09-02


'''A script that ingests MAVEN in-situ KP files.
'''
import argparse
import os
import sys
import time
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import Index
from maven_database import database, models
from maven_utilities.singleton import SingleInstance
from maven_in_situ_kp_file_ingester import utilities
from maven_utilities import mail, maven_log, constants
from maven_status import job, MAVEN_SDC_COMPONENT
from importlib import reload
tmp_table_suffix = '_tmp'


def parse_arguments(arguments):
    '''Parses arguments from main()'''
    parser = argparse.ArgumentParser()
    parser.add_argument('src_dir',
                        help='''MAVEN IUVS KP file ingester for the
                                directories beneath src_dir''')
    return parser.parse_args(arguments)


def get_identifier(arguments):
    '''Gets an identifier for the singleton'''
    return ''.join([os.path.basename(arguments[0]).replace('/', '-'),
                    ':',
                    arguments[1].replace('/', '-')])


def reload_table_definitions(suffix, tables):
    '''Method used to dynamically reload the MAVEN models.
    This will update the provided tables (assuming they use the TableNameMixin)
    to have a suffix of the provided suffix
    Arguments:
        suffix - The table name suffix to use
        tables - The set of tables to retrieve and return
    Returns:
        A list of Tables retrieved from the SQLAlchemy Metadata after reloading the model'''
    os.environ[constants.MAVEN_DB_TABLE_SUFFIX] = suffix
    reload(database)
    reload(models)

    reloaded_tables = []
    for t in tables:
        reloaded_tables.append(database.Base.metadata.tables[t.__tablename__])
    return reloaded_tables

# In dependency order (least to most)
kp_tables = [models.InSituKeyParametersData,
             models.InSituKpQueryParameter,
             models.KpFilesMetadata,
             ]


def main(arguments):
    
    args = parse_arguments(arguments[1:])

    identifier = get_identifier(arguments)
    _ = SingleInstance(identifier)  # allow only one instance to parse this directory tree
    utilities.logger.info('''starting maven in-situ KP file ingester for
                             the directories beneath %s''' % args.src_dir)
    
    # Switch to temp tables
    insitu_tables = reload_table_definitions(tmp_table_suffix, kp_tables)
    # Create temp tables
    for t in insitu_tables:
        try:
            t.drop(database.engine)  # Drop temp table if exists
        except ProgrammingError:  # Table didn't exist
            pass
    for t in reversed(insitu_tables):
        t.create(database.engine)

    utilities.logger.info('Starting kp ingest')

    # Ingest
    utilities.ingest_in_situ_kp_data(args.src_dir)

    utilities.logger.info('Starting id/timetag index build')
    # Add indexes
    timetag = time.time()
    Index('data_qpid_time_ix_{0}'.format(timetag),
          models.InSituKeyParametersData.in_situ_kp_query_parameters_id,
          models.InSituKeyParametersData.timetag).create(database.engine)

    utilities.logger.info('Starting timetag index build')
    Index('data_timetag_ix_{0}'.format(timetag),
          models.InSituKeyParametersData.timetag).create(database.engine)
    
    utilities.logger.info('Starting table rename')

    # Switch to production tables
    insitu_tables = reload_table_definitions('', kp_tables)

    for t in insitu_tables:
        try:
            t.drop(database.engine)
        except ProgrammingError:  # Table didn't exist
            pass

    with database.engine.connect() as conn:
        for t in insitu_tables:
            # SQLAlchemy doesn't have a builtin means to alter tables
            conn.execute('ALTER TABLE {0} RENAME TO {1}'.format(t.name + tmp_table_suffix, t.name))
            conn.execute('GRANT ALL privileges ON TABLE {0} to mavenmgr'.format(t.name))
            conn.execute('GRANT SELECT ON TABLE {0} to mavendb'.format(t.name))
            conn.execute('GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE {0} to mavenpro'.format(t.name))

    utilities.logger.info('ending maven in-situ KP file ingester for the directories beneath %s' % sys.argv[1])

    utilities.logger.info('''ending maven in-situ KP file ingester for the
                                directories beneath %s''' % args.src_dir)

if __name__ == "__main__":
    with job.StatusCronJob(MAVEN_SDC_COMPONENT.KP_INGESTER) as job:
        job.run(proc=main,
                proc_args={'arguments': sys.argv})