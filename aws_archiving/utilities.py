'''Utilities shared by various modules in this project.

Created on Sep 29, 2014

@author: Mike Dorey
@author: Kim Kokkonen (reorganized, added restore functions)
'''
import sys
import os
import logging
import boto
import json
from datetime import tzinfo, timedelta
import smtplib
import datetime
import hashlib
import tarfile
from optparse import OptionParser
from subprocess import Popen, PIPE, CalledProcessError

from .singleton import SingleInstance
from . import models
from . import config
from .models import GlacierVault, GlacierArchive, GlacierArchiveFile, ErrorLog


# define log formatter
formatter = logging.Formatter('%(asctime)s - %(name)s: %(levelname)s %(message)s')
# set up a console logger
ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(logging.DEBUG)
# set up a file logger
if not os.path.isdir(config.root_logging_directory):
    os.makedirs(config.root_logging_directory)
fh = logging.FileHandler(os.path.join(config.root_logging_directory, 'archive.log'))
fh.setFormatter(formatter)
fh.setLevel(logging.DEBUG)

# add handlers to root logger
logging.getLogger('').addHandler(ch)
logging.getLogger('').addHandler(fh)

# get logger for this module
logger = logging.getLogger('aws_archiving.utilities')
logger.setLevel(logging.INFO)


def get_vault(region_name, vault_name):
    '''Returns the vault that can then be used
    to upload files.

    Arguments
        region_name - name of the region where the vault is stored.
        vault_name - name of the vault in the region

    Note:
        This code assumes ~/.boto holds the correct credentials.
    '''
    conn = boto.connect_glacier(region_name=region_name)
    return conn.get_vault(vault_name)


class UTC(tzinfo):
    """UTC defined with a format that matches the needs of AWS"""

    def utcoffset(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return "Z"

    def dst(self, dt):
        return timedelta(0)


def submit_inventory_request(aws_region, vault_name, start_date, end_date):
    '''Load inventory of all archives between start_date and end_date.
    If both are None, download all.
    If end_date is None, download all later than start_date.
    If start specified, download all within the range.
    '''
    # get the vault
    vault = get_vault(region_name=aws_region, vault_name=vault_name)
    if vault is None:
        print (f'Cannot get vault with region_name {aws_region}, vault_name {vault_name}')
        return None

    # get the vault inventory up through restore_date
    # there is a problem using start_date and end_date: https://github.com/boto/boto/issues/2154
    # make sure the boto version has this fix
    job_id = vault.retrieve_inventory(start_date=start_date, end_date=end_date)
    # job_id = vault.retrieve_inventory()
    print (job_id)
    return job_id


def download_inventory(aws_region, vault_name, restore_root_directory, job_id):
    '''Download the inventory file if the job is done. Returns a status code,
       0 if the file was downloaded, nonzero if the job isn't found or isn't
       complete.
    '''
    vault = get_vault(region_name=aws_region, vault_name=vault_name)
    if vault is None:
        print (f'Cannot get vault with region_name {aws_region}, vault_name {vault_name}')
        return (255, None)

    job = vault.get_job(job_id)
    if job is None:
        print (f'Job does not exist [{job_id}]')
        return (255, None)
    elif not job.completed:
        print (f'Job not yet completed [{job_id}], status [{job.status_code}]')
        return (254, None)
    else:
        fn = os.path.join(restore_root_directory, 'inventory_%s.json' % job_id)
        job.download_to_file(fn, verify_hashes=False)
        print (fn)
        return (0, fn)


def submit_download_request(aws_region, vault_name, restore_root_directory, inventory_fn):
    '''Submit requests to download all the archives specified in inventory file.
       inventory_file is assumed in the json format used as the default by Glacier.
       Returns a status code,
       0 if the file was downloaded, nonzero if the job isn't found or isn't
       complete.
    '''
    vault = get_vault(region_name=aws_region, vault_name=vault_name)
    if vault is None:
        print (f'Cannot get vault with region_name {aws_region}, vault_name {vault_name}')
        return (255, None)

    with open(inventory_fn) as f:
        jdict = json.load(f)

    file_dict = {}
    tsize = 0
    for d in jdict['ArchiveList']:
        file_dict[d['ArchiveId']] = d['ArchiveDescription']
        tsize += d['Size']
        print (f'{d["Size"]}, { d["ArchiveDescription"]}')
    print (f'total files: {len(file_dict)}, total bytes: {tsize}')

    job_ids = []
    for aid in file_dict:
        job = vault.retrieve_archive(aid, description=file_dict[aid])
        job_ids.append(job.id)

    # just for looking at in the log
    print (job_ids)

    return (0, job_ids)


def download_files(aws_region, vault_name, restore_root_directory):
    '''Check for running or completed jobs in the specified vault.
    As long as one is complete and has an associated archive, download
    that archive to a local file based on restore_root_directory
    and the job description tarball name. If no remaining jobs in
    progress, return 254, else return the number in progress up to 253.
    '''
    vault = get_vault(region_name=aws_region, vault_name=vault_name)
    if vault is None:
        print (f'Cannot get vault with region_name {aws_region}, vault_name {vault_name}')
        return 255

    # loop at least once checking jobs
    list_again = True
    in_progress = 0
    while list_again:
        jobs = vault.list_jobs()
        if len(jobs) == 0:
            return 254
        list_again = False
        in_progress = 0
        for job in jobs:
            if job.archive_id is not None:
                if job.status_code == 'Succeeded':
                    fn = os.path.join(restore_root_directory, os.path.basename(job.description))
                    if not os.path.isfile(fn):
                        job.download_to_file(fn)
                        print (f'Downloaded {job.archive_size} bytes for file {fn}')
                        list_again = True
                elif job.status_code == 'InProgress':
                    if in_progress < 253:
                        in_progress += 1

    if in_progress > 0:
        return in_progress
    else:
        # no more jobs in progress
        return 254


def get_dict_of_files_in_vault(aws_region, vault_name):
    '''Fetches information from the database. Returns a 
    dict of the files that have already been sent to the
    Glacier vault. Keys of the dict are full absolute
    paths to the files and values are dicts that hold
    information about the file in the archive.

    Only the information from the latest archiving of a file 
    ends up in the dict.
    '''
    vault = GlacierVault.get(GlacierVault.aws_region == aws_region, GlacierVault.vault_name == vault_name)
    archives = [a for a in vault.archives.order_by(GlacierArchive.when_archived)]
    d = {}
    for archive in archives:
        for file_ in archive.files:
            d[file_.file_path] = {'file_size': file_.file_size,
                                  'md5_hash': file_.md5_hash,
                                  'modification_time': file_.modification_time}
    return d


def path_is_excluded(path, excluded_directories):
    '''Returns True if the specified path is one of the excluded directories.'''
    path_padded = clean_directory_name(path)
    for ed in excluded_directories:
        if path_padded.startswith(ed):
            logger.info('Excluding directory %s', path_padded)
            return True
    return False


def add_file_to_dict(fn, result_dict):
    '''Add filename fn to result_dict, using a named tuple containing file_size and modification_time.

    Returns the file_size.
    '''
    file_size = os.path.getsize(fn)
    result_dict[fn] = {'file_size': file_size,
                       'modification_time': datetime.datetime.fromtimestamp(os.path.getmtime(fn))}
    return file_size


def equal_datetimes(date_from_db, date_from_disk):
    '''Compare two datetime objects for equality, with some room for slop. The two
    datetimes should reference the same file. In the current implementation, the microseconds
    of each datetime are set to 0. This behavior is consistent with fractional second
    truncation that occurs with file copy utilities such as cpio, and with the behavior
    of rsync when it compares file timestamps.

    Arguments
        date_from_db - the datetime stored (at full resolution) in the sqlite database.
        date_from_disk - the datetime found on the filesystem (at full resolution).

    Returns
        True if datetimes are considered equal, else False.
    '''
    return date_from_db.replace(microsecond=0) == date_from_disk.replace(microsecond=0)


def get_dict_of_new_or_changed_files(root_directory, aws_region, vault_name, excluded_directories=[]):
    '''Returns a dict of files that have never been sent to the Glacier
    vault or have changed since they were last sent to the vault.

    Information about the inventory in Glacier is fetched from the database.

    Arguments
        root_directory - top of the directory tree that will be walked to
            find new or changed files
        aws_region - name of the Amazon Web Services region
        vault_name - name of the vault
        excluded_directories - optional. Holds list of directories that are to be excluded from the archive.
    '''
    logger.info("Getting dict of files in database")
    vault_dict = get_dict_of_files_in_vault(aws_region, vault_name)
    logger.info("Got %d files from database" % len(vault_dict))
    logger.info("Getting dict of files to archive now")
    result_dict = {}
    total_size = 0
    for path, dirs, files in os.walk(root_directory, onerror=log_error, topdown=True, followlinks=False):

        # Filter out excluded directories.
        dirs[:] = [os.path.join(path, d) for d in dirs]
        dirs[:] = [d for d in dirs if not path_is_excluded(d, excluded_directories)]

        if not path_is_excluded(path, excluded_directories):
            for f in files:
                fn = os.path.join(path, f)
                if os.path.isfile(fn):
                    try:
                        if not fn in vault_dict:
                            total_size += add_file_to_dict(fn, result_dict)
                        else:  # it has been sent to Glacier before
                            v = vault_dict[fn]
                            if v['file_size'] != os.path.getsize(fn):  # file size has changed
                                total_size += add_file_to_dict(fn, result_dict)
                            elif not equal_datetimes(v['modification_time'],
                                                     datetime.datetime.fromtimestamp(os.path.getmtime(fn))):
                                total_size += add_file_to_dict(fn, result_dict)
#                             else:
#                                 with open(fn) as f:
#                                     md5_hash = hashlib.md5(f.read()).hexdigest()
#                                 if v['md5_hash'] != md5_hash:
#                                     total_size += add_file_to_dict(fn, result_dict)

                    except Exception as e:
                        log_error(e)

    logger.info("Got %d files of total size %d to archive" % (len(result_dict), total_size))
    return result_dict


def log_error(error):
    error_st = str(error)
    utc_now = datetime.datetime.utcnow()

    if 'Permission denied' not in error_st:
        # print the stack trace
        logger.exception(error)
        # log the error in the database.
        ErrorLog.create(error_message=error_st, when_logged=utc_now)

        # log to file and console
        logger.error(error_st)


def send_email(message):
    '''Sends an email.

    Arguments
        message - contents of the email
    '''
    sender = config.email_sender
    receivers = config.email_recipients
    message_strings = ['From: %s' % sender]
    to_string = '<' + '>, <'.join(receivers) + '>'
    message_strings.append('To: %s' % to_string)
    message_strings.append('Subject: Glacier Archiving Message')
    message = '\n'.join(message_strings) + '\n\n' + message
    smtpObj = smtplib.SMTP('localhost')
    smtpObj.sendmail(sender, receivers, message)


def build_tarball(file_dict):
    '''Returns the name of a tarball that holds the files in the 
    list of file paths, up to raw size config.max_tar_size.

    Argument
        file_dict - dictionary of files: keys are absolute paths,
            values are a named tuple including file_size.
    '''
    utcnow = datetime.datetime.utcnow()
    tarball_ext = 'tar.gz' if config.use_gzip else 'tar'
    tarball_filename = '%s-sdc-archive-%04d-%02d-%02d-%02d-%02d-%02d.%s' % (
        config.mission_name,
        utcnow.year,
        utcnow.month,
        utcnow.day,
        utcnow.hour,
        utcnow.minute,
        utcnow.second,
        tarball_ext)
    if not os.path.isdir(config.tarball_directory):
        os.makedirs(config.tarball_directory)
    tarball_path = os.path.join(config.tarball_directory, tarball_filename)
    # sort files for better tar organization
    file_paths = sorted(file_dict.keys())
    raw_size = 0
    file_count = 0

    # note -C / allows using relative paths
    # to avoid: Removing leading `/' from member names
    # --ignore-failed-read avoids failing if file has disappeared since list was made
    tar_opts = '-cvzf' if config.use_gzip else '-cvf'
    initial_args = ['/bin/tar', '-C', '/', '--ignore-failed-read', tar_opts, tarball_path]
    args = list(initial_args)
    # max arg len from `getconf ARG_MAX`
    # divide by 2 to avoid error from pushing too close
    max_arg_len = 2621440 / 2
    arg_len = len(' '.join(args))
    for p in file_paths:
        arg_len += len(p)
        if arg_len > max_arg_len:
            break
        # remove leading slash
        args.append(p[1:])
        file_size = file_dict[p]['file_size']
        raw_size += file_size
        file_count += 1
        del file_dict[p]
        if raw_size > config.max_tar_size:
            if file_size > 100000000:
                # log if the file is over 100MB
                logger.info('Last file in tar has size %d and path %s:', file_size, p)
            break

    p = Popen(args, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    returncode = p.returncode
    if returncode > 1:
        # exit code 1 means 'Some files differ'
        # see https://www.gnu.org/software/tar/manual/html_section/tar_19.html
        logger.error('Error reported by tar:\n%s', error)
        # don't report the complete file list, it's too long
        raise CalledProcessError(returncode, ' '.join(initial_args))
    if error:
        logger.warn('Warning reported by tar:\n%s', error)
    if output:
        logger.debug('Added files:\n%s', output)

    logger.info("Created tarball %s with file_count %d, raw_size %d, tar_size %d" %
                (tarball_path, file_count, raw_size, os.path.getsize(tarball_path)))
    return tarball_path


def send_archive_to_glacier(tarball_filename, aws_region, vault_name):
    '''Sends the archive to Glacier. 

    Returns the archive id created at Glacier.

    Arguments
        tarball_filename - full absolute path to the tarball to send to Glacier
        aws_region - AWS region that holds the vault
        vault_name - name of the Glacier vault
    '''
    assert os.path.isfile(tarball_filename)
    retries = 0
    max_retries = 3
    while True:
        vault = get_vault(region_name=aws_region, vault_name=vault_name)
        try:
            archive_id = vault.upload_archive(tarball_filename, tarball_filename)
            break
        except Exception:
            # log_error(e)
            if retries < max_retries:
                logger.warn('Retrying upload of %s', tarball_filename)
            else:
                raise
        retries += 1
    logger.info("Uploaded archive %s with id %s" % (tarball_filename, archive_id))
    return archive_id


def get_md5_hash(fn):
    '''Determine the md5 hexdigest of a file that might be larger than memory.
    '''
    hasher = hashlib.md5()
    with open(fn, "rb") as f:
        for chunk in iter(lambda: f.read(1048576), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def add_archive_to_inventory(tarball_filename, glacier_id, aws_region, vault_name):
    '''Adds the files that are in the tarball to the local inventory.

    Arguments
        tarball_filename - name of the tarball (archive in Glacier-speak) that was sent to glacier
        glacier_id - id of the archive in the Glacier vault
        aws_region - name of the AWS region, e.g. us-west-2
        vault_name - name of the Glacier vault, e.g. maven-sdc-vault
    '''
    vault = GlacierVault.get(GlacierVault.aws_region == aws_region, GlacierVault.vault_name == vault_name)
    with models.db.transaction():
        archive = GlacierArchive.create(glacier_vault=vault,
                                        glacier_id=glacier_id,
                                        glacier_description=tarball_filename,
                                        md5_hash=get_md5_hash(tarball_filename),
                                        when_archived=datetime.datetime.utcnow())
        tarfile_opts = 'r:gz' if config.use_gzip else 'r'
        with tarfile.open(tarball_filename, tarfile_opts) as tf:
            for fn in tf.getnames():
                full_fn = os.path.join('/', fn)
                try:
                    modification_time = datetime.datetime.fromtimestamp(os.path.getmtime(full_fn))
                    _ = GlacierArchiveFile.create(glacier_archive=archive,
                                                  file_path=full_fn,
                                                  file_size=os.path.getsize(full_fn),
                                                  md5_hash=get_md5_hash(full_fn),
                                                  modification_time=modification_time)
                except (IOError, OSError) as e:
                    if 'No such file' in str(e):
                        logger.warn(e)
                    else:
                        log_error(e)
    logger.info('Saved database inventory for tarball %s' % tarball_filename)


def clean_tarball(tarball_pathname):
    '''After a tarball has been built and its contents added to the local database,
    and optionally uploaded, clean up the tarball by deleting it or moving it to
    an appropriate location.
    '''
    if config.use_upload:
        if config.delete_tarball:
            if os.path.isfile(tarball_pathname):
                os.remove(tarball_pathname)
        else:
            # move to "uploaded" directory
            if not os.path.isdir(config.uploaded_tarball_directory):
                os.makedirs(config.uploaded_tarball_directory)
            os.rename(tarball_pathname, os.path.join(config.uploaded_tarball_directory, os.path.basename(tarball_pathname)))
    else:
        # move to "completed" directory, it's not uploaded yet
        if not os.path.isdir(config.completed_tarball_directory):
            os.makedirs(config.completed_tarball_directory)
        os.rename(tarball_pathname, os.path.join(config.completed_tarball_directory, os.path.basename(tarball_pathname)))


def archive_files(root_directory, aws_region, vault_name, excluded_directories=[]):
    '''Performs all the steps for uploading files to Glacier:
        - finds files that are new or changed compared to previous archives
        - builds one or more tarballs of those files
        - uploads the tarballs to glacier
        - adds entries for each uploaded file to the sqlite local inventory

    The code does an upload as soon as the files in the tarball exceed config.max_tar_size bytes, 
    and then starts another archive if needed.

    Tarballs created locally are kept.

    Arguments
        root_directory - top of the directory tree that will be walked to
            find new or changed files
        aws_region - name of the Amazon Web Services region
        vault_name - name of the vault
        excluded_directories - optional. Holds list of directories that are to be excluded from the archive.
    '''
    try:
        new_or_changed_dict = get_dict_of_new_or_changed_files(root_directory, aws_region,
                                                               vault_name, excluded_directories)
        while len(new_or_changed_dict) > 0:
            tarball_pathname = build_tarball(new_or_changed_dict)
            if config.use_upload:
                glacier_archive_id = send_archive_to_glacier(tarball_pathname, aws_region, vault_name)
            else:
                # assume it's going to s3 with glacier backing
                glacier_archive_id = config.amazon_s3_prefix + os.path.basename(tarball_pathname)
            add_archive_to_inventory(tarball_pathname, glacier_archive_id, aws_region, vault_name)
            clean_tarball(tarball_pathname)
    except Exception as e:
        log_error(e)


def clean_directory_name(dn):
    '''Check the given directory name dn. If it is not None 
    and its length is greater than zero, append a trailing 
    slash if it does not already end with one.
    '''
    if dn:
        return dn if dn[-1] == os.sep else dn + os.sep
    else:
        return dn


def main(argv):
    '''Parse command line arguments and call archive_files() to perform archiving. 
    Argv is a list of argument strings, typically from sys.argv.

    This function is in the utilities module to aid in testing.
    '''
    parser = OptionParser()
    parser.add_option('-r', '--aws-region', type="string", dest="aws_region", default="us-west-2",
                      help="AWS region where the Glacier vault lives, default is us-west-2")

    excluded_directories = []

    def exclude_directory_callback(option, opt, value, parser):
        excluded_directories.append(clean_directory_name(value))
    parser.add_option('-e', '--exclude-directory',
                      type='string',
                      action='callback',
                      callback=exclude_directory_callback,
                      help='Exclude this directory from the archive')

    def exclude_directories_callback(option, opt, value, parser):
        with open(value, "r") as efile:
            for edir in efile.readlines():
                if len(edir.strip()) > 0 and edir[0] != '#':
                    excluded_directories.append(clean_directory_name(edir.strip()))
    parser.add_option('-d', '--exclude-directories',
                      type='string',
                      action='callback',
                      callback=exclude_directories_callback,
                      help='Exclude a list of directories in a given file')

    options, args = parser.parse_args(argv[1:])
    if len(args) != 2:
        print(f'Usage: {argv} <vault name> <root directory>')
        sys.exit(-1)

    vault_name = args[0]
    root_directory = args[1]
    aws_region = options.aws_region
    signature = '-'.join([sys.argv[0], vault_name, root_directory, aws_region])
    signature = signature.replace('/', '-')
    _ = SingleInstance(signature)
    archive_files(root_directory, aws_region, vault_name, excluded_directories)
