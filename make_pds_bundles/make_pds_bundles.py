# -*- coding: utf-8 -*-
"""
make_pds_bundles.py

Python script to get all of the files in the passed directory.
Later, this will be just one definition within a larger script
  that performs the bundling of the data to go to the PDS.

Created on Wed May 14 13:03 2014
Last Revised on Tue Aug 12 12:52 2014

@author: mcgouldrick, bstaley, bharter
"""
#
# Import relevant global modules
#
import hashlib
import os
import re
import tarfile
import sys
import json
import csv
import imp

from collections import namedtuple
from gzip import GzipFile
from dateutil.parser import parse
import logging
from maven_utilities import time_utilities, constants
from maven_utilities.utilities import is_compressed_format
from maven_status import MAVEN_SDC_EVENTS, MAVEN_SDC_COMPONENT
from maven_status.status import add_status
from . import results, utilities, config, archive_progress, file_finder

logger = logging.getLogger('maven.make_pds_bundles.make_pds_bundles.log')
dry_run_logger = logging.getLogger('maven.make_pds_bundles.make_pds_bundles.dryrunlog')
direct_out_logger = logging.getLogger('maven.make_pds_bundles.make_pds_bundles.directlog')

ChecksumData = namedtuple('ChecksumData', 'checksum fully_qualified_filename')
output_file_pattern = re.compile(
    r'([a-zA-Z0-9\-]+)_([a-zA-Z0-9\-]+)_([0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2})_([0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2})\.([a-zA-Z]+)(\.([0-9]+)|)')


def input_dates(date1, date2):
    '''
    Take the two input dates from the command line.  Verify which one
    is older (doesn't matter: we'll grab everything between early date
    and later date regardless of the order).  Create a mini dictionary
    that maps the start date and end date to strings

    Turns the string input into a datetime object with the UTC timezone
    '''

    date1_datetime = parse(date1)
    date1_utc = time_utilities.to_utc_tz(date1_datetime)
    date2_datetime = parse(date2)
    date2_utc = time_utilities.to_utc_tz(date2_datetime)

    if (date1_utc > date2_utc):
        return {'start': date2_utc, 'end': date1_utc}
    return {'start': date1_utc, 'end': date2_utc}


def generate_transfer_manifest(inst_dict, files):
    lid_dict = inst_dict['lid']
    lidvid = []  # Initialize the LIDVID list
    fname = []  # Initialize the filename list

    for label_filename in files:  # For all label files
        file_matched = False

        for i in lid_dict:
            regex = re.compile(lid_dict[i])
            if regex.search(label_filename) is not None:  # Does the label file meet the accepted formats
                vers_search = re.findall('v\d{2,3}', label_filename)  # Does the label file have a version/revision
                revis_search = re.findall('r\d{2,3}', label_filename)
                if len(vers_search) > 0:
                    vers = int(re.sub("_v", "", re.findall('_v\d{2,3}', label_filename)[0]))
                else:
                    vers = None
                if len(revis_search) > 0:
                    revis = int(re.sub("_r", "", re.findall('_r\d{2,3}', label_filename)[0]))
                else:
                    revis = None
                # Is this a compressed file?
                filename_base, filename_ext = os.path.splitext(label_filename)
                if filename_ext == '.gz':
                    # Strip the .gz as the tar process will untar prior to bundle
                    fname.append(filename_base)
                else:
                    fname.append(label_filename)
                if vers is not None and revis is not None:
                    lidvid.append(i + '::%i.%i' % (vers, revis))
                else:
                    lidvid.append(i)
                file_matched = True
                break
        if not file_matched:
            logger.warning('Label file %s did not match any accepted formats', label_filename)

    return list(zip(lidvid, fname))


def print_transfer_manifest(lidvids, target_dir, manifest_file):
    max_len = 0
    for i in lidvids:
        max_len = max([max_len, len(i[0])])
    #
    outfile = os.path.join(target_dir, manifest_file)

    if not os.path.exists(os.path.dirname(outfile)):
        os.makedirs(os.path.dirname(outfile))
    with open(outfile, 'wb') as f:
        #
        #  Define the print format string and write the lines to the file
        fmt_str = ('%%-%is %%s\n' % max_len)
        for i in lidvids:
            f.write((fmt_str % i).encode())
        f.flush()


def print_checksum_manifest(target_dir, checksums, checksum_file):
    '''
    Print the checksum manifest list
    '''
    outfile = os.path.join(target_dir, checksum_file)
    with open(outfile, 'wb') as f:
        for i in checksums:
            f.write(('{0}  {1}\n').format(i.checksum, i.fully_qualified_filename).encode())
        f.flush()


def get_md5_checksums(files):
    '''
    Calculates the md5 checksum for each file in the bundle
    We will want to also generate an output file containing this information.
    For now, just return a list containing each checksum value
    '''
    checksum = []  # Initialize list of checksums
    for ifile in files:  # Loop over all files to be bundled
        m = hashlib.md5()  # Initialize the md5 checksum value
        fd = open(ifile, 'rb')  # Open the current file
        content = fd.readlines()  # Read each lines in the file(?)
        fd.close()  # Close the file
        for eachline in content:
            m.update(eachline)  # Adds value of each line to checksum for file

        checksum.append(m.hexdigest())
    return checksum


def get_instrument_filters(instrument, override_inst_config):
    override_config = imp.load_source("override_config", override_inst_config) if override_inst_config else None
    if override_config is not None and (
            config.ScienceFileSearchParameters._fields != override_config.ScienceFileSearchParameters._fields):
        mismatch_error_msg = 'Error: Science File Parameters do not match! \n' + 'Current parameters: ' + str(
            config.ScienceFileSearchParameters._fields) + '\n User supplied Parameters: ' + str(
            override_config.ScienceFileSearchParameters._fields)
        logger.error(mismatch_error_msg)
        raise Exception(mismatch_error_msg)
    return config.instrument_config[instrument] if not override_inst_config else override_config.instrument_config[
        instrument]


# pylint: disable=W0613
def tar_bundle_files(target_dir,
                     bundle_files,
                     lidvids,
                     bundle_file,
                     dry_run,
                     progress,
                     skip_no_label_files=False):
    '''
    This will:
        1. Define tar output file name as <inst>_<start>_<end>.tar
        2. Go to maven/data/sci/<inst>
        3. Loop through files, appending each to tarball
        4. Zip the tarball
        5. Rename the tarball from *.tar.gz to *.tgz
    '''

    checksums = []

    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    #
    #  Define the name of the tarfile
    #
    tarfile_name = os.path.join(target_dir, bundle_file)
    lidvid_files = [os.path.splitext(os.path.basename(value[1]))[0] for value in lidvids]
    # progress.add_handler(StatusProgressHandler(component=MAVEN_SDC_COMPONENT.PDS_ARCHIVER),
    #                     .25)

    
    # Loop through the files in the bundle
    if not dry_run:
        with tarfile.open(tarfile_name, 'w:gz') as tar_file:
            for file_to_tar in bundle_files:
                # strip version/revision and extension
                base_file = os.path.basename(file_to_tar)
                base_name = re.sub(r'_v\d{2,3}_r\d{2,3}.*', '', os.path.splitext(os.path.basename(file_to_tar))[0])
                if base_name not in lidvid_files:
                    # logger.warning('No label file base name %s was found in the manifest.', base_name)
                    if skip_no_label_files:
                        continue

                # Is this a compressed file?
                if is_compressed_format(file_to_tar):
                    with GzipFile(file_to_tar, 'rb') as gzip:
                        try:
                            # Create unzipped file
                            tmp_filename = os.path.splitext(file_to_tar)[0]
                            # logger.info('Adding %s to tar', tmp_filename)
                            with open(tmp_filename, 'wb') as tmp_file:
                                tmp_file.write(gzip.read())
                                # Add unzipped file
                            tar_file.add(tmp_filename)
                            checksums.append(ChecksumData(get_md5_checksums([tmp_filename])[0], tmp_filename))
                            # Remove unzipped tar from file system
                            os.remove(tmp_filename)
                            # logger.info('Done adding %s to tar as %s', file_to_tar, tmp_filename)
                            # progress.complete_unit(file_to_tar)
                        except IOError as e:
                            logger.error('Error adding %s : %s', file_to_tar, str(e))
                            # progress.error_unit(file_to_tar)
                else:
                    try:
                        # logger.info('Adding %s to tar', file_to_tar)
                        tar_file.add(file_to_tar,
                                     recursive=False)
                        checksums.append(ChecksumData(get_md5_checksums([file_to_tar])[0], file_to_tar))
                        # logger.info('Done adding %s to tar', file_to_tar)
                        # progress.complete_unit(file_to_tar)
                    except IOError as e:
                        logger.error('Error adding %s : %s', file_to_tar, str(e))
                        # progress.error_unit(file_to_tar)

        logger.info('Done building tar %s', tarfile_name)

    else:
        dry_run_logger.info('gzip %s', tarfile_name)
        for i in bundle_files:
            base_name = re.sub(r'_v\d{2,3}_r\d{2,3}.*', '', os.path.splitext(os.path.basename(i))[0])
            checksums.append(ChecksumData(get_md5_checksums([i])[0], i))
            if base_name not in lidvid_files:
                dry_run_logger.warning('No label file base name %s was found in the manifest.', base_name)
            dry_run_logger.info('Adding %s to tar', i)

    return checksums


def create_event_archive_bundle(target_dir, from_dt, to_dt, bundle_file, manifest_file, checksum_file, file_version,
                                dry_run):
    '''Method used to create the PDS bundle for MAVEN SDC and OPs events
    Arguments:
        target_dir - The full path to the output directory
        from_dt - The start time for the bundle generation
        to_dt - The end time for the bundle generation
        bundle_file - The name of the bundle file
        manifest_file - The name of the manifest file
        checksum_file - The name of the checksum file
        file_version - The bundle version
        dry_run - True if run is a dry run, False otherwise
    '''
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # Generated Events
    events = utilities.query_for_events(from_dt, to_dt)
    ops_events = utilities.query_for_ops_events(from_dt, to_dt)

    event_file_name = get_filename(file_prefix='maven',
                                   data_type='events',
                                   start_dt=from_dt,
                                   end_dt=to_dt,
                                   file_extension='csv',
                                   version=file_version)
    event_file_name = os.path.join(target_dir, event_file_name)
    ops_file_name = get_filename(file_prefix='ops',
                                 data_type='events',
                                 start_dt=from_dt,
                                 end_dt=to_dt,
                                 file_extension='csv',
                                 version=file_version)
    ops_file_name = os.path.join(target_dir, ops_file_name)

    with open(event_file_name, 'w+') as event_file:
        event_file.write(utilities.generate_sdc_events_csv(events).getvalue())

    with open(ops_file_name, 'w+') as event_file:
        event_file.write(utilities.generate_ops_events_csv(ops_events).getvalue())

    transfer_file = os.path.join(target_dir, manifest_file)
    # From http://pds.nasa.gov/pds4/doc/sr/v1/StdRef_4.0.8_130507_v1.pdf
    # LID format
    #           urn:nasa:pds:<bundle_id>
    #           urn:nasa:pds:<bundle_id>:<collection_id>
    #           urn:nasa:pds:<bundle_id>:<collection_id>:<product_id>
    # Or for versioned files (e.g LIDVID)
    #           urn:nasa:pds:<bundle_id>::<version_id>
    #           urn:nasa:pds:<bundle_id>:<collection_id>::<version_id>
    #           urn:nasa:pds:<bundle_id>:<collection_id>:<product_id>::<version_id>
    sdc_events_lid = 'urn:nasa:pds:maven.events:data.events:%s' % os.path.splitext(os.path.basename(event_file_name))[0]
    ops_events_lid = 'urn:nasa:pds:maven.events:data.events:%s' % os.path.splitext(os.path.basename(ops_file_name))[0]

    with open(transfer_file, 'w') as transfer_file:
        transfer_file.write('%s %s' % (sdc_events_lid, event_file_name))
        transfer_file.write('%s %s' % (ops_events_lid, ops_file_name))

    checksums = tar_bundle_files(target_dir,
                                 [event_file_name, ops_file_name],
                                 [(sdc_events_lid, event_file_name),
                                  (ops_events_lid, ops_file_name)],
                                 bundle_file,
                                 dry_run,
                                 archive_progress.ArchiveProgress([event_file_name], prefix='Events'))

    print_checksum_manifest(target_dir, checksums, checksum_file)


def create_ancillary_archive_bundle(target_dir,
                                    from_dt,
                                    end_dt,
                                    bundle_file,
                                    manifest_file,
                                    checksum_file,
                                    products,
                                    extensions,
                                    dry_run):
    '''Method used to create the PDS bundle for MAVEN ancillary data
    Arguments:
        target_dir - The full path to the output directory
        from_dt - The start for the bundle generation
        end_dt - The end for the bundle generation
        bundle_file - The name of the bundle file
        manifest_file - The name of the manifest file
        checksum_file - The name of the checksum file
        products - The products to be included in the bundle
        extensions - The file extensions to be included in the bundle
        dry_run - True if run is a dry run, False otherwise
    '''
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    ancillary_metadata = utilities.query_for_ancillary_files(product_list=products,
                                                             extension_list=extensions,
                                                             from_dt=from_dt,
                                                             to_dt=end_dt)

    progress = archive_progress.ArchiveProgress([f.file_name for f in ancillary_metadata], prefix='Ancillary')

    transfer_file = os.path.join(target_dir, manifest_file)
    bundle_files = []

    lids = []
    for next_anc_metadata in ancillary_metadata:
        fully_qualified_file_name = '/'.join([next_anc_metadata.directory_path, next_anc_metadata.file_name])

        if not os.path.isfile(fully_qualified_file_name):
            logger.warning('%s was not found on the filesystem!  Continuing', fully_qualified_file_name)
            continue

        bundle_files.append(fully_qualified_file_name)
        # From http://pds.nasa.gov/pds4/doc/sr/v1/StdRef_4.0.8_130507_v1.pdf
        # LID format
        #           urn:nasa:pds:<bundle_id>
        #           urn:nasa:pds:<bundle_id>:<collection_id>
        #           urn:nasa:pds:<bundle_id>:<collection_id>:<product_id>
        # Or for versioned files (e.g LIDVID)
        #           urn:nasa:pds:<bundle_id>::<version_id>
        #           urn:nasa:pds:<bundle_id>:<collection_id>::<version_id>
        #           urn:nasa:pds:<bundle_id>:<collection_id>:<product_id>::<version_id>
        anc_lid = 'urn:nasa:pds:maven.anc.drf:data.%s' % next_anc_metadata.base_name
        lids.append(anc_lid)
    max_len = 0
    for i in lids:
        max_len = max([max_len, len(i)])

    fmr_str = ('%%-%is  %%s\n' % max_len)
    lid_zip = zip(lids, bundle_files)

    with open(transfer_file, 'w') as transfer_file:
        for i in lid_zip:
            transfer_file.write(fmr_str % i)

        checksums = tar_bundle_files(target_dir, bundle_files, zip(lids, bundle_files), bundle_file, dry_run, progress)

        print_checksum_manifest(target_dir, checksums, checksum_file)


def print_instrument_dictionary(instruments=config.all_key):
    '''Method used to dump the instrument lid configuration for the provided list of instruments
    Arguments:
        instruments - A list of instruments for which to dump lid information
    '''
    if config.all_key in instruments:
        instruments = list(config.instrument_config.keys())
    for instrument in [inst for inst in instruments if inst in config.instrument_dictionary.keys()]:
        direct_out_logger.info(str(config.instrument_dictionary[instrument]))


def print_instrument_config(instruments=config.all_key):
    ''' Method used to dump the configuration information for the provided list of instruments
    Arguments:
        instruments - A list of instruments for which to dump configuration information
    '''
    if config.all_key in instruments:
        instruments = list(config.instrument_config.keys())
    for instrument in instruments:
        direct_out_logger.info(str(config.instrument_config[instrument]))


def report_missing_sdc_file(missing_file):
    ''' Method used to report any missing sdc files from the inventory
    Arguments:
        missing_file - The name of the missing file
    '''
    desc = '{} Not found in the SDC!'.format(missing_file)
    add_status(component_id=MAVEN_SDC_COMPONENT.PDS_ARCHIVER,
               event_id=MAVEN_SDC_EVENTS.STATUS,
               summary=missing_file,
               description=desc)


def run_report(start, end, instruments, override_inst_config=None):
    ''' Method used to produce a report that contains the files that will be part of
    the archive as well as the size of the individual files along with the total
    archive size (pre compressed).
    Arguments:
      start - The start time to report
      end - The end time to report
      instruments - The list of instruments to process
      override_inst_config - File where instrument_config variable to be used is located
    '''
    report_line_width = 100

    if config.all_key in instruments:
        instruments = list(config.instrument_config.keys())
        logger.info('All instruments will be reported %s', instruments)

    date_range = input_dates(start, end)

    for next_instrument in instruments:
        next_instrument_filters = get_instrument_filters(next_instrument, override_inst_config)

        metadata_for_files_in_archive = utilities.get_latest_science_metadata(
            instrument_list=[next_instrument_filters.instrument],
            level_list=next_instrument_filters.levels,
            extension_list=next_instrument_filters.exts,
            plan_list=next_instrument_filters.plans,
            from_dt=date_range['start'],
            to_dt=date_range['end'],
            version=next_instrument_filters.ver,
            revision=next_instrument_filters.rev)

        archive_size_in_bytes = sum([meta.file_size for meta in metadata_for_files_in_archive])
        archive_num_files = len(metadata_for_files_in_archive)

        direct_out_logger.info('=' * report_line_width)
        direct_out_logger.info('={{0: ^{0}}}'.format(report_line_width).format(next_instrument))
        direct_out_logger.info('=' * report_line_width)
        direct_out_logger.info('={{0: ^{0}}} '.format(report_line_width).format('Archive info'))
        direct_out_logger.info(
            '= {{0: <{0}}} {{1: <{1}}}'.format(report_line_width // 2, report_line_width // 2).format('Size in bytes',
                                                                                                      archive_size_in_bytes))
        direct_out_logger.info(
            '= {{0: <{0}}} {{1: <{1}}}'.format(report_line_width // 2, report_line_width // 2).format('Num files',
                                                                                                      archive_num_files))
        direct_out_logger.info('=' * report_line_width)
        direct_out_logger.info('={{0: ^{0}}}'.format(report_line_width).format('Query info'))
        direct_out_logger.info(
            '= {{0: <{0}}} {{1: <{1}}}'.format(report_line_width // 2, report_line_width // 2).format('Instrument',
                                                                                                      next_instrument_filters.instrument))
        direct_out_logger.info(
            '= {{0: <{0}}} {{1}}'.format(report_line_width // 2).format('Levels', next_instrument_filters.levels))
        direct_out_logger.info(
            '= {{0: <{0}}} {{1}}'.format(report_line_width // 2).format('Extensions', next_instrument_filters.exts))
        direct_out_logger.info(
            '= {{0: <{0}}} {{1}}'.format(report_line_width // 2).format('Plans', next_instrument_filters.plans))
        direct_out_logger.info(
            '= {{0: <{0}}} {{1: <{1}}}'.format(report_line_width // 2, report_line_width // 2).format('From',
                                                                                                      date_range[
                                                                                                          'start']))
        direct_out_logger.info(
            '= {{0: <{0}}} {{1: <{1}}}'.format(report_line_width // 2, report_line_width // 2).format('To', date_range[
                'end']))
        direct_out_logger.info(
            '= {{0: <{0}}} {{1: <{1}}}'.format(report_line_width // 2, report_line_width // 2).format('Version',
                                                                                                      'Latest' if next_instrument_filters.ver is None else next_instrument_filters.ver))
        direct_out_logger.info(
            '= {{0: <{0}}} {{1: <{1}}}'.format(report_line_width // 2, report_line_width // 2).format('Revision',
                                                                                                      'Latest' if next_instrument_filters.rev is None else next_instrument_filters.rev))
        direct_out_logger.info('=' * report_line_width)
        direct_out_logger.info('= {{0: ^{0}}}'.format(report_line_width).format('Files in archive'))
        for f in metadata_for_files_in_archive:
            direct_out_logger.info(
                '= {{0: <{0}}} {{1: <{1}}}'.format(report_line_width // 2, report_line_width // 2).format(f.file_name,
                                                                                                          f.file_size))
        direct_out_logger.info('=' * report_line_width)


def generate_bundle_file_names(inst, from_dt, to_dt, file_version):
    '''Method used to generate the bundle,checksum and manifest file names'''

    bundle_file = get_filename(file_prefix=inst + '-pds',
                               data_type='bundle',
                               start_dt=from_dt,
                               end_dt=to_dt,
                               file_extension='tgz',
                               version=file_version)
    checksum_file = get_filename(file_prefix='checksum',
                                 data_type='manifest',
                                 start_dt=from_dt,
                                 end_dt=to_dt,
                                 file_extension='txt',
                                 version=file_version)
    manifest_file = get_filename(file_prefix='transfer',
                                 data_type='manifest',
                                 start_dt=from_dt,
                                 end_dt=to_dt,
                                 file_extension='txt',
                                 version=file_version)

    return bundle_file, checksum_file, manifest_file


def run_archive(start, end, instruments, root_dir, dry_run, user_notes=None, override_inst_config=None,
                skip_no_label_files=False):
    ''' Method used to run an archive.  This will either generate the PDS4 compliant artifacts
    or print a report.
    Arguments:
        start - The start time to archive
        end - The end time to archive
        instruments = The list of instruments to process
        root_dir - The root path used for providing a basis for generated archive files
        dry_run - If true, print what would be archived, otherwise perform the archive
        user_notes - Optional notes about the archive to be stored in the recorded results
        override_inst_config - File where instrument_config variable to be used is located
        skip_no_label_files - If true, don't bundle files that don't have a corresponding label file
    '''
    if config.all_key in instruments:
        instruments = list(config.instrument_config.keys())
        logger.info('All instruments will be processed %s', instruments)

    start = time_utilities.to_utc_tz(parse(start))
    end = time_utilities.to_utc_tz(parse(end))
    bundle_file=None

    if start > end:
        start, end = end, start

    logger.info(
        'Running PDS archive with the following arguments:\n\t start:%s\n\t end:%s\n\t instruments:%s\n\t root_dir:%s\n\t dry_run:%s' %
        (start, end, instruments, root_dir, dry_run))

    generation_time = time_utilities.utc_now()

    next_instrument_filters = None
    for next_instrument in instruments:
        logger.info('Running PDS archive for %s', next_instrument)
        logger.info(
            'Instrument configuration for %s :\n\t %s' % (next_instrument, config.instrument_config[next_instrument]))
        try:
            archive_directory = None
            current_file_version = None
            next_instrument_filters = get_instrument_filters(next_instrument, override_inst_config)
            bundle_file, checksum_file, manifest_file = None, None, None

            if next_instrument == config.ancillary_key:
                archive_directory = os.path.join(root_dir, 'maven/data/arc', 'anc')
                current_file_version = get_latest_version(archive_directory) + 1
                bundle_file, checksum_file, manifest_file = generate_bundle_file_names(
                    next_instrument_filters.instrument, start, end, current_file_version)

                create_ancillary_archive_bundle(archive_directory,
                                                start,
                                                end,
                                                bundle_file,
                                                manifest_file,
                                                checksum_file,
                                                next_instrument_filters.plans,
                                                next_instrument_filters.exts,
                                                dry_run)

            elif next_instrument == config.event_key:
                archive_directory = os.path.join(root_dir, 'maven/data/arc', 'events')
                current_file_version = get_latest_version(archive_directory) + 1
                bundle_file, checksum_file, manifest_file = generate_bundle_file_names(
                    next_instrument_filters.instrument, start, end, current_file_version)
                create_event_archive_bundle(archive_directory,
                                            start,
                                            end,
                                            bundle_file,
                                            manifest_file,
                                            checksum_file,
                                            current_file_version,
                                            dry_run)
            elif 'metadata' in next_instrument_filters.plans:
                instrument_dir = next_instrument_filters.instrument
                metadata_path = os.path.join(root_dir, f'maven/data/sci/{instrument_dir}/metadata')

                bundle_files=[]
                if os.path.isdir(metadata_path):
                    bundle_files = [os.path.join(os.path.abspath(metadata_path), f) for f in os.listdir(metadata_path)]

                archive_directory = os.path.join(root_dir, 'maven/data/arc', instrument_dir)
                current_file_version = get_latest_version(archive_directory) + 1
                bundle_file, checksum_file, manifest_file = generate_bundle_file_names(next_instrument, start, end,
                                                                                       current_file_version)
                instrument_lidvids = []
                print_transfer_manifest(instrument_lidvids,
                                        archive_directory,
                                        manifest_file)
                
                checksums = tar_bundle_files(archive_directory,
                                             bundle_files,
                                             instrument_lidvids,
                                             bundle_file,
                                             dry_run,
                                             archive_progress.ArchiveProgress(bundle_files, prefix=next_instrument),
                                             skip_no_label_files)

                print_checksum_manifest(archive_directory,
                                        checksums,
                                        checksum_file)
            else:

                file_generator = file_finder.InventoryFileFinder(next_instrument_filters.as_inv_file,
                                                                 results_from_dt=start,
                                                                 results_to_dt=end,
                                                                 results_not_extensions=['xml'],
                                                                 uprev_inv_file=next_instrument_filters.uprev_inv_file,
                                                                 missing_file_handler=report_missing_sdc_file) if next_instrument_filters.as_inv_file else \
                    file_finder.ScienceQueryFileFinder(instrument_list=[next_instrument_filters.instrument],
                                                       plan_list=next_instrument_filters.plans,
                                                       level_list=next_instrument_filters.levels,
                                                       extension_list=next_instrument_filters.exts,
                                                       file_name=next_instrument_filters.file_name,
                                                       from_dt=start,
                                                       to_dt=end,
                                                       version=next_instrument_filters.ver,
                                                       revision=next_instrument_filters.rev)

                # get the latest files that match the filters for this instrument
                bundle_files = list(file_generator.generate())

                label_file_generator = file_finder.InventoryFileFinder(
                    next_instrument_filters.as_inv_file,
                    results_from_dt=start,
                    results_to_dt=end,
                    results_version=next_instrument_filters.label_ver,
                    results_revision=next_instrument_filters.label_rev,
                    results_extensions=['xml'],
                    uprev_inv_file=next_instrument_filters.uprev_inv_file,
                    missing_file_handler=report_missing_sdc_file) if next_instrument_filters.as_inv_file else \
                    file_finder.ScienceQueryFileFinder(instrument_list=[next_instrument_filters.instrument],
                                                       plan_list=next_instrument_filters.plans,
                                                       level_list=next_instrument_filters.levels,
                                                       extension_list=['xml'],
                                                       from_dt=start,
                                                       to_dt=end)

                label_files = list(label_file_generator.generate())

                # combine science data files and label files
                bundle_files = set(label_files) | set(bundle_files)

                progress = archive_progress.ArchiveProgress(bundle_files, prefix=next_instrument)

                if bundle_files is None:
                    msg = 'Skipping instrument %s, no archivable files' % next_instrument_filters.instrument
                    logger.info(msg)
                    continue

                for f in bundle_files:
                    if not os.path.isfile(f):
                        logger.warning('%s was not found on the filesystem!  Continuing', f)

                instrument_lidvids = []

                if next_instrument_filters.instrument in config.instrument_dictionary:
                    instrument_lidvids = generate_transfer_manifest(
                        config.instrument_dictionary[next_instrument_filters.instrument],
                        label_files)

                instrument_dir = next_instrument_filters.instrument
                if instrument_dir == 'kp':
                    instrument_dir = os.path.join(instrument_dir, next_instrument_filters.levels[0])

                archive_directory = os.path.join(root_dir, 'maven/data/arc', instrument_dir)
                current_file_version = get_latest_version(archive_directory) + 1
                bundle_file, checksum_file, manifest_file = generate_bundle_file_names(next_instrument, start, end,
                                                                                       current_file_version)

                print_transfer_manifest(instrument_lidvids,
                                        archive_directory,
                                        manifest_file)

                checksums = tar_bundle_files(archive_directory,
                                             bundle_files,
                                             instrument_lidvids,
                                             bundle_file,
                                             dry_run,
                                             progress,
                                             skip_no_label_files)

                print_checksum_manifest(archive_directory,
                                        checksums,
                                        checksum_file)

            results.record_results(generation_time=generation_time,
                                   start_time=start,
                                   end_time=end,
                                   command_line=' '.join(sys.argv),
                                   configuration=json.dumps(next_instrument_filters) if next_instrument_filters else '',
                                   dry_run=dry_run,
                                   result_directory=archive_directory,
                                   bundle_file_name=bundle_file,
                                   manifest_file_name=manifest_file,
                                   checksum_file_name=checksum_file,
                                   result_version=current_file_version,
                                   generation_result=results.GENERATION_SUCCESS,
                                   notes=user_notes)
        except Exception as e:
            import traceback
            logger.exception('PDS Bundle Failure!')
            results.record_results(generation_time=generation_time,
                                   start_time=start,
                                   end_time=end,
                                   command_line=' '.join(sys.argv),
                                   configuration=json.dumps(next_instrument_filters) if next_instrument_filters else '',
                                   dry_run=dry_run,
                                   result_directory=archive_directory,
                                   bundle_file_name=bundle_file,
                                   manifest_file_name=manifest_file,
                                   checksum_file_name=checksum_file,
                                   result_version=current_file_version,
                                   generation_result=results.GENERATION_FAILURE,
                                   notes=user_notes if user_notes else '' + ' ERROR: ' + traceback.format_exc())
            raise e


def get_latest_version(directory):
    '''Method used to get the latest archive file version
    Arguments:
        directory - The directory to search for file versions
    '''
    running_latest_version = 0
    if not os.path.isdir(directory):
        return running_latest_version
    for f in [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]:
        file_match = output_file_pattern.match(f)
        if file_match is not None:
            file_version = file_match.groups()[6]
            if file_version is not None:
                if int(file_version) > running_latest_version:
                    running_latest_version = int(file_version)
    return running_latest_version


def get_filename(file_prefix, data_type, start_dt, end_dt, file_extension, version=None):
    '''Method used to generate a correct archive file name
    Arguments:
        file_prefix - The first part of the file name
        data_type - The second part of the file name
        start_dt -The third part of the file name
        end_dt - The fourth part of the file name
        file_extension - The fifth part of the file name
        version - The last part of the file name
    Returns:
        A correct file name
    '''
    file_name = os.path.join('_'.join([file_prefix,
                                       data_type,
                                       start_dt.strftime(config.filename_time_format),
                                       end_dt.strftime(config.filename_time_format)]) + '.' + file_extension)
    if version is not None:
        file_name = file_name + '.' + str(version)

    assert output_file_pattern.match(file_name) is not None, '%s did not match' % (file_name)
    return file_name
