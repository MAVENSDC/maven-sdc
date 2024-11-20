'''
Modules used to build TRK bundles

Created on Sep 23, 2015
@author: bstaley
'''

import os
import tarfile
import pytz
import hashlib
from datetime import datetime, timedelta
import shelve
import logging

from maven_utilities import time_utilities
from maven_database.models import AncillaryFilesMetadata
from maven_status import status, MAVEN_SDC_COMPONENT, MAVEN_SDC_EVENTS
from maven_utilities import constants

shelf_file = '/maven/mavenpro/.trk_bundle_manifest'

logger = logging.getLogger('maven.ingest_anc_files.build_trk_bundle.log')


def get_yydoy(from_dt):
    '''Method used to return a string in the form YYDOY for the provided datetime'''
    time_tuple = from_dt.timetuple()
    return '{0}{1:03d}'.format(str(time_tuple.tm_year)[2:], time_tuple.tm_yday)


def get_dt_from_yydoy(year, doy):
    '''Method used to return a UTC aware datetime provided the year and doy'''
    dt = datetime(year, 1, 1) + timedelta(doy - 1)
    return dt.replace(tzinfo=pytz.UTC)


def get_manifest(manifest_file=None, writeback=True, persist=False):
    '''Method used to deserialize the shelve file into a manifest
    Arguments:
        manifest_file - The fully qualified path to the shelve file
    Returns:
        The opened shelve
    '''
    if manifest_file is None:
        manifest_file = shelf_file
    if not os.path.isfile(manifest_file):
        touch_shelf = shelve.open(manifest_file)
        touch_shelf.close()
    return shelve.open(manifest_file, writeback=writeback, flag='c' if persist else 'r')


def dump_manifest(manifest):
    '''Method used to output the entire manifest'''
    for _n in manifest:
        print ('{}:{}'.format(_n, manifest[_n]))


def get_manifest_entry(bundle_file):
    '''Method used to generate an manifest entry for the trk bundle generated'''
    manifest_entry = {'file_name': bundle_file,
                      'md5': hashlib.md5(open(bundle_file, 'rb').read()).hexdigest()}

    return manifest_entry


def build_bundle_latest(end_dt, out_dir, day_increment, manifest_file):
    '''Method used to build a TRK bundle using the last entry in the manifest
    as the start date and UTC now as the end date'''
    m = get_manifest(manifest_file=manifest_file, writeback=False)
    # determine last generation time
    last_generation_dt = constants.MAVEN_MISSION_START if len(m.values()) == 0 else max([next_entry['end'] for next_entry in m.values()])
    m.close()

    if end_dt > last_generation_dt:
        # run bundle generation
        build_bundles(last_generation_dt, end_dt, out_dir, day_increment, manifest_file)


def build_bundle_full(end_dt, out_dir, day_increment, manifest_file):
    '''Method used to run a full TRK bundle.  This will compare the manifest with
    what is currently on disk and rebuild the bundle if things have changed
    Arguments:
        end_dt - the end time to use when querying for trk files
        out_dir - output directory for the bundle file
        day_increment - Fractional day between TRK bundles
        manifest_file - The manifest shelve file to use
    '''

    # run a latest to build any new bundles
    build_bundle_latest(end_dt, out_dir, day_increment, manifest_file)

    manifest = get_manifest(manifest_file=manifest_file, writeback=False)
    bundles_to_rebuild = reconcile_bundle(manifest=manifest)

    re_bundles = [(os.path.split(d)[0], s, e) for (d, s, e) in bundles_to_rebuild]

    for directory, start, end in re_bundles:
        build_bundle(start_dt=start,
                     end_dt=end,
                     out_dir=directory,
                     manifest_file=manifest_file)


def reconcile_bundle(manifest):
    '''Method used to determine if the manifest matches what is on disk
    Arguments:
        manifest - The TRK bundle manifest deserialized via get_manifest
    Returns:
        A list of manifest keys that represent the bundles that are out of sync
    '''
    bundles_to_regenerate = []
    for next_manifest_key in manifest:
        next_manifest = manifest[next_manifest_key]
        logger.info('Checking manifest %s', next_manifest)
        if not os.path.isfile(next_manifest_key):
            bundles_to_regenerate.append((next_manifest_key, next_manifest['start'], next_manifest['end']))
            continue
        trk_files = AncillaryFilesMetadata.query.filter(AncillaryFilesMetadata.file_extension == '234')\
                                                .filter(AncillaryFilesMetadata.start_date >= next_manifest['start'])\
                                                .filter(AncillaryFilesMetadata.start_date < next_manifest['end']).all()
        db_trk_filenames = [os.path.join(m.directory_path, m.file_name) for m in trk_files]
        bundle_filenames = [m['file_name'] for m in next_manifest['bundled_files']]

        if set(db_trk_filenames) != set(bundle_filenames):
            bundles_to_regenerate.append((next_manifest_key, next_manifest['start'], next_manifest['end']))
    return bundles_to_regenerate


def build_bundles(start_dt, end_dt, out_dir, day_increment, manifest_file=None):
    '''Method used to build a set of TRK bundle that vary based on day_increment
    Arguments:
        start_dt - the start time to use when querying for trk files
        end_dt - the end time to use when querying for trk files
        out_dir - output directory for the bundle file
        day_increment - Fractional day between TRK bundles
        manifest_file - The manifest shelve file to use
    '''
    while start_dt <= end_dt:
        next_end = min(end_dt, start_dt + timedelta(day_increment))
        build_bundle(start_dt, next_end, out_dir, manifest_file)
        start_dt += timedelta(day_increment)


def build_bundle(start_dt, end_dt, out_dir, manifest_file=None):
    '''Method used to build a TRK bundle
    Arguments:
        start_dt - the start time to use when querying for trk files
        end_dt - the end time to use when querying for trk files
        out_dir - output directory for the bundle file
    '''
    manifest = get_manifest(manifest_file=manifest_file, persist=True)
    bundled_files = []
    bundle_name = 'mvn_anc_trk_{0}_{1}.tgz'.format(get_yydoy(start_dt),
                                                   get_yydoy(end_dt))
    bundle_name = os.path.join(out_dir, bundle_name)
    logger.info('working on bundle %s', bundle_name)
    with tarfile.open(bundle_name, 'w:gz') as tar_file:
        try:
            trk_anc_files = AncillaryFilesMetadata.query\
                .filter(AncillaryFilesMetadata.file_extension == '234')\
                .filter(AncillaryFilesMetadata.start_date >= start_dt)\
                .filter(AncillaryFilesMetadata.start_date < end_dt).all()

            for next_trk in trk_anc_files:
                file_to_add = os.path.join(next_trk.directory_path, next_trk.file_name)
                logger.debug('Adding %s to tar', file_to_add)
                tar_file.add(file_to_add, recursive=False)
                bundled_files.append(get_manifest_entry(file_to_add))
                logger.info('successfully added %s to bundle', file_to_add)
        except Exception:
            logger.exception('Exception occurred while building the TRK bundle')
            status.add_exception_status(component_id=MAVEN_SDC_COMPONENT.ANC_INGESTER,
                                        event_id=MAVEN_SDC_EVENTS.FAIL,
                                        summary='Exception occurred while building the TRK bundle')

        manifest[bundle_name] = {'generation_time': time_utilities.utc_now(),
                                 'start': start_dt,
                                 'end': end_dt,
                                 'bundled_files': bundled_files}

        manifest.close()
