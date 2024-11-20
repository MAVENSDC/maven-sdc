'''
Created on Dec 3, 2015

@author: bstaley
'''
import os
import shutil
import logging
import datetime
import operator
import time
import pytz
import re
import subprocess
from calendar import monthrange
from sqlalchemy import or_, not_
from urllib.request import urlopen, HTTPError
from . import config
from maven_database.models import AncillaryFilesMetadata, ScienceFilesMetadata
from maven_utilities import time_utilities
from maven_database import db_session

PDS_RELEASE_FLAG = 'RELEASED'
PDS_SUCCESS_FLAG = 'SUCCESS'
ANC_DIR = 'anc'
SPICE_DIR = 'anc/spice'
ORB_DIR = 'anc/orb'
OPTG_DIR = 'anc/optg'
SCI_DIR = 'sci'
SDC_DIR = 'sdc'
MISC_DIR = 'misc'
EUV_FLARE_DIR = 'sci/euv/swx'
EUV_L2B_DIR = 'sci/euv/l2b'
EUV_L3B_DIR = 'sci/euv/l3b'

directory_time_format = '%Y%m%dT%H%M%S'
site_sim_dir_pattern = re.compile('site-([0-9]{8}T[0-9]{6})')

logger = logging.getLogger('maven.maven_public.utilities.log')

public_release_window_date_start = datetime.datetime(2013, 12, 1, tzinfo=pytz.UTC)

current_month = datetime.datetime.now().month
current_day = datetime.datetime.now().day
current_year = datetime.datetime.now().year
if current_month == 1 or (current_month==2 and current_day < 15):
    public_release_window_date_end = datetime.datetime(current_year-1, 8, 15, tzinfo=pytz.UTC)
elif (current_month==2 and current_day >= 15) or current_month == 3 or current_month==4 or (current_month==5 and current_day < 15):
     public_release_window_date_end = datetime.datetime(current_year-1, 11, 15, tzinfo=pytz.UTC)
elif (current_month==5 and current_day >= 15) or current_month == 6 or current_month==7 or (current_month==8 and current_day < 15):
     public_release_window_date_end = datetime.datetime(current_year, 2, 15, tzinfo=pytz.UTC)
elif (current_month==8 and current_day >= 15) or current_month == 9 or current_month==10 or (current_month==11 and current_day < 15):
     public_release_window_date_end = datetime.datetime(current_year, 5, 15, tzinfo=pytz.UTC)
elif (current_month==11 and current_day >= 15) or current_month == 12:
    public_release_window_date_end = datetime.datetime(current_year, 8, 15, tzinfo=pytz.UTC)


class SciQueryMetadata():

    def __init__(self,
                 instrument_list=None,
                 plan_list=None,
                 file_extension_list=None,
                 start_date=None,
                 end_date=None,
                 version=None,
                 latest=False):
        '''Class used to generate the source files for a given Science Query
        Arguments:
            instrument_list - The list of instruments to query
            plan_list - The list of plans to query
            file_extension_list - The list of ext to query
            start_date - The start date to query
            end_date - The end date to query
            version - The version to query
            latest - filter on the LATEST (as determined by version/revision) files
        '''
        self.instrument_list = instrument_list if instrument_list else []
        self.plan_list = plan_list if plan_list else []
        self.file_extension_list = file_extension_list if file_extension_list else []
        self.start_date = start_date
        self.end_date = end_date
        self.version = version
        self.latest = latest

    def generate(self):
        '''Generator for the found source file names'''
        for _next in self.get_query().yield_per(10):
            yield (os.path.join(_next[0], _next[1]))

    def get_query(self):
        '''Method used to generate the SQL query for ancillary data
    Returns:
        An SQL query for the requested Ancillary metadata
    '''
        query = db_session.query(
            ScienceFilesMetadata.directory_path, ScienceFilesMetadata.file_name)

        instrument_filter = []
        for inst in self.instrument_list:
            instrument_filter.append(
                operator.eq(ScienceFilesMetadata.instrument, inst))
        query = query.filter(or_(*instrument_filter))

        plan_filter = []
        for plan in self.plan_list:
            plan_filter.append(operator.eq(ScienceFilesMetadata.plan, plan))
        query = query.filter(or_(*plan_filter))

        ext_filter = []
        for ext in self.file_extension_list:
            ext_filter.append(
                operator.eq(ScienceFilesMetadata.file_extension, ext))
        query = query.filter(or_(*ext_filter))

        if self.start_date:
            query = query.filter(
                ScienceFilesMetadata.timetag >= self.start_date)

        if self.end_date:
            query = query.filter(ScienceFilesMetadata.timetag < self.end_date)

        if self.version:
            query = query.filter(ScienceFilesMetadata.version == self.version)

        if self.latest:
            query = query.order_by(ScienceFilesMetadata.file_root.desc(),
                                   ScienceFilesMetadata.version.desc()).distinct(ScienceFilesMetadata.file_root)

        return query

    def __str__(self):
        return 'SciQueryMetadata -'\
            '\n\t instruments : {0}'\
            '\n\t plans : {1}'\
            '\n\t file exts : {2}'\
            '\n\t start : {3}'\
            '\n\t end : {4}'\
            '\n\t version : {5}'\
            '\n\t latest : {6}'.format(self.instrument_list,
                                       self.plan_list,
                                       self.file_extension_list,
                                       self.start_date,
                                       self.end_date,
                                       self.version,
                                       self.latest)


class AncQueryMetadata():

    def __init__(self,
                 base_name=None,
                 product=None,
                 file_extension_list=None,
                 start_date=None,
                 end_date=None,
                 version=None,
                 latest=False,
                 spice=False):
        '''Class used to generate the source files for a given Ancillary Query
        Arguments:
            base_name - The base name to query
            product - The product to query
            file_extension_list - The list of ext to query
            start_date - The start date to query
            end_date - The end date to query
            version - The version to query
            latest - filter on the LATEST (as determined by version) files
            spice - include the SPICE data
            '''

        self.base_name = base_name
        self.product = product
        self.file_extension_list = file_extension_list if file_extension_list else []
        self.start_date = start_date
        self.end_date = end_date
        self.version = version
        self.latest = latest
        self.spice = spice

    def generate(self):
        '''Generator for the found source file names'''
        for _next in self.get_query().yield_per(10):
            yield (os.path.join(_next[0], _next[1]))

    def get_query(self):
        '''Method used to generate the SQL query for ancillary data
    Returns:
        An SQL query for the requested Ancillary metadata
        '''
        query = db_session.query(
            AncillaryFilesMetadata.directory_path, AncillaryFilesMetadata.file_name)

        if self.base_name:
            query = query.filter(
                AncillaryFilesMetadata.base_name == self.base_name)

        if self.product:
            query = query.filter(
                AncillaryFilesMetadata.product == self.product)

        file_extension_filter = []
        for ext in self.file_extension_list:
            file_extension_filter.append(
                operator.eq(AncillaryFilesMetadata.file_extension, ext))
        query = query.filter(or_(*file_extension_filter))

        if self.end_date:
            query = query.filter(
                AncillaryFilesMetadata.start_date < self.end_date)

        if self.start_date:
            query = query.filter(
                AncillaryFilesMetadata.end_date >= self.start_date)

        if self.version:
            query = query.filter(
                AncillaryFilesMetadata.version == self.version)

        if self.latest:
            query = query.order_by(AncillaryFilesMetadata.file_root.desc(),
                                   AncillaryFilesMetadata.version.desc()).distinct(AncillaryFilesMetadata.file_root)
        if not self.spice:
            query = query.filter(not_(AncillaryFilesMetadata.file_extension.in_(['tls', 'tsc', 'tpc', 'bsp', 'bc', 'ti', 'tf'])))

        return query

    def __str__(self):
        return 'AncQueryMetadata -'\
            '\n\t base : {0}'\
            '\n\t product : {1}'\
            '\n\t file exts : {2}'\
            '\n\t start : {3}'\
            '\n\t end : {4}'\
            '\n\t version : {5}'\
            '\n\t latest : {6}'.format(self.base_name,
                                       self.product,
                                       self.file_extension_list,
                                       self.start_date,
                                       self.end_date,
                                       self.version,
                                       self.latest)


class PdsQueryMetadata():

    def __init__(self, base_url, data_product_urls, ppi=False):
        '''
        Class used to generate the list of source files using PDS release information
        Arguments:
            base_url - The base url of the PDS node to search
            data_product_urls - A list of urls where the data products reside
            ppi - Whether or not we're querying the ppi website (instead of atmospheres)
        '''

        self.pds_base_url = base_url
        self.pds_data_product_urls = data_product_urls
        self.ppi = ppi

    @staticmethod
    def _parse_file_name(pds_file):
        '''Helper method used to parse a file name found on the pds (with no extension)
        Arguments:
            file : The file to parse
        Returns:
            (base_name,version,revision)
        '''
        m = config.base_verrev_regex.match(pds_file)

        if m is None:
            raise ValueError("{0} doesn't meet regex {1}".format(m, config.base_verrev_regex))

        base_name = m.group('base')
        version = int(m.group('version'))
        revision = int(m.group('revision'))

        return (base_name, (version, revision))

    @staticmethod
    def _compare(sfmd, file_tuple):
        '''Method used to compare a ScienceFileMetadata object and a inventory tuple
        Arguments:
            sfmd - The ScienceFileMetadata object to compare
            file_tuple - The pds file tuple (base_name,(version,revision))
        Returns:
            -1 if sfmd < inv_tuple, 1 if sfmd > inv_tuple 0 if sfmd == inv_tuple
        '''

        sfmd_base_name = sfmd.file_root.split('.')[0]

        return ((sfmd_base_name, (sfmd.version, sfmd.revision)) > file_tuple) - ((sfmd_base_name, (sfmd.version, sfmd.revision)) < file_tuple)


    @staticmethod
    def _valid_label_file(sfmd, file_tuple):
        '''Method used to determine if the file_tuple represents the correct
        label file for sfmd.
        Arguments:
            sfmd - The ScienceFileMetadata object to compare
            file_tuple - The pds file tuple (base_name,(version,revision))
        Returns:
            True if the file_tuple is the correct label file, else False
        '''

        #Check that the file roots are the same
        if sfmd.file_root.split('.')[0] != file_tuple[0]:
            return False

        #Check that the file is actually a label file
        if sfmd.file_extension=='xml':
            if sfmd.instrument == 'iuv' or sfmd.level == 'iuvs':
                if sfmd.version == 1 and sfmd.revision == 0:
                    return True
            else:
                #Check that version/revision match
                if sfmd.version == file_tuple[1][0] and sfmd.revision == file_tuple[1][1]:
                    return True
        return False

    def generate(self):
        for data_product_url in self.pds_data_product_urls:
            pds_released_files = []

            # Find full url to the data product
            base_search_url = self.pds_base_url + data_product_url

            # Walk through the URL to find the list of released data
            for i in range(0,3):
                science_files = walk_through_pds(base_search_url, ppi=self.ppi)

                if len(science_files) == 0:
                    logger.warning("No released files were found for the data type %s", data_product_url)
                    time.sleep(180)
                    continue
                else:
                    break

            # turn inventory files into tuple of the form (basename, (version, revision))
            for f in science_files:
                try:
                    file_tuple = self._parse_file_name(f)
                except ValueError:
                    logger.warning("The file [%s] doesn't appear to be a science file.  Skipping.", f)
                    continue

                pds_released_files.append(file_tuple)

            if len(pds_released_files) == 0:
                logger.warning("No released files were found for the data type %s", data_product_url)
                continue

            pds_released_files.sort()

            next_pds_file = pds_released_files.pop(0)

            try:
                for f in pds_released_files:
                    query = ScienceFilesMetadata.query.filter(operator.ge(ScienceFilesMetadata.file_root, f[0]))
                    query = query.filter(operator.le(ScienceFilesMetadata.file_root, f[0] + 'Z'))
                    for next_sfmd in query.yield_per(20):
                        # Check if next_sfmd is the label file for the next_pds_file
                        # With the above query ordering, label files always come first, so don't move on to the next
                        # file in pds_released files yet.
                        if self._valid_label_file(next_sfmd, f):
                            #logger.info("Releasing: %s", next_sfmd.file_name)
                            yield os.path.join(next_sfmd.directory_path, next_sfmd.file_name)

                        if self._compare(next_sfmd, f) == 0:
                            #logger.info("Releasing: %s", next_sfmd.file_name)
                            yield os.path.join(next_sfmd.directory_path, next_sfmd.file_name)
            except IndexError:  # reached end of next_inv_entry list
                pass



class SystemFileQueryMetadata():
    '''A QueryMetadata class for any System file type'''

    def __init__(self,
                 root_dir,
                 base_name_pattern='.*',
                 start_date=None,
                 end_date=None,
                 get_date=None,
                 child_directories=True):
        '''Method use to initialize the query against the file system
        Arguments:
            root_dir - The root directory to begin searching
            base_name_pattern - The regular expression used to find files of interest
            start_date - The start date to query
            end_date - The end date to query
            get_date - A function used to determine a file time. The function is expected
                       to be of the form get_date(<filename>) which returns a datetime.
            child_directories - True => recursively search child directories, False => don't recursively search
        '''

        def system_file_get_date(file_name):
            if not os.path.isfile(file_name):
                return None
            return datetime.datetime.fromtimestamp(os.path.getmtime(file_name)).replace(tzinfo=pytz.UTC)

        self.base_name_regex = re.compile(base_name_pattern)
        self.root_dir = root_dir
        self.child_directories = child_directories
        self.get_date = get_date if get_date else system_file_get_date
        self.start_date = start_date
        self.end_date = end_date

    def generate(self):
        '''Generator for the found source file names'''
        for root, _, files in os.walk(self.root_dir):
            for file_name in files:
                if self.base_name_regex.match(file_name):
                    fn = os.path.join(root, file_name)
                    file_dt = self.get_date(fn)
                    if self.start_date and file_dt < self.start_date:
                        continue
                    if self.end_date and file_dt >= self.end_date:
                        continue
                    yield fn
            if not self.child_directories:
                break

    def __str__(self):
        return 'SystemFileQueryMetadata -'\
            '\n\t root : {0}'\
            '\n\t base : {1}'\
            '\n\t start : {2}'\
            '\n\t end : {3}'\
            '\n\t get_date : {4}'\
            '\n\t child dirs : {5}'.format(self.root_dir,
                                           self.base_name_regex.pattern,
                                           self.start_date,
                                           self.end_date,
                                           self.get_date,
                                           self.child_directories)


orbit_file_pattern = 'maven_orb_rec_(?P<start>[\d]{6})_(?P<end>[\d]{6}).*'
trk_bundle_pattern = 'mvn_anc_trk_(?P<start>[\d]{5})_(?P<end>[\d]{5}).tgz'


def orbit_file_time_getter(file_name):
    '''Orbit file specific time getter.  Uses the difference of the
    start and end times in the name to determine the file time'''
    file_regex = re.compile(orbit_file_pattern)

    m = file_regex.match(os.path.basename(file_name))

    if not m:
        return None

    return time_utilities.to_utc_tz(datetime.datetime.strptime(m.group('end'), '%y%m%d'))

default_source_generators = [PdsQueryMetadata(config.atmos_search_url, config.atmos_data_urls),
                             PdsQueryMetadata(config.ppi_search_url, config.ppi_data_urls, ppi=True),
                             AncQueryMetadata(base_name='sci_anc',
                                              start_date=public_release_window_date_start,
                                              end_date=public_release_window_date_end),
                             #                            AncQueryMetadata(base_name='sci_anc',
                             #                                         product='eps',
                             #                                         end_date=public_release_window_date_end),
                             AncQueryMetadata(base_name='mvn',
                                              start_date=public_release_window_date_start,
                                              end_date=public_release_window_date_end,
                                              latest=True),
                             AncQueryMetadata(base_name='anc_sci',
                                              start_date=None,
                                              end_date=public_release_window_date_end,
                                              latest=True),
                             AncQueryMetadata(base_name='mvn_sc',
                                              start_date=public_release_window_date_start,
                                              end_date=public_release_window_date_end,
                                              latest=True),
                             AncQueryMetadata(base_name='mvn_app',
                                              start_date=public_release_window_date_start,
                                              end_date=public_release_window_date_end,
                                              latest=True),
                             AncQueryMetadata(base_name='mvn',
                                              product='iuv_all',
                                              start_date=None,
                                              end_date=public_release_window_date_end,
                                              latest=True),
                             AncQueryMetadata(base_name='mvn',
                                              product='rec',
                                              start_date=public_release_window_date_start,
                                              end_date=public_release_window_date_end,
                                              latest=True),
                             AncQueryMetadata(base_name='mvn',
                                              product='tst',
                                              start_date=public_release_window_date_start,
                                              end_date=public_release_window_date_end,
                                              latest=True),
                             # AncQueryMetadata(base_name='optg',
                             #                 start_date=public_release_window_date_start,
                             #                 end_date=public_release_window_date_end,
                             #                 latest=True),
                             AncQueryMetadata(base_name='spk',
                                              start_date=public_release_window_date_start,
                                              end_date=public_release_window_date_end,
                                              latest=True),
                             AncQueryMetadata(base_name='MVN',
                                              start_date=public_release_window_date_start,
                                              end_date=public_release_window_date_end,
                                              latest=True),
                             AncQueryMetadata(product='noHdr',
                                              start_date=None,
                                              end_date=public_release_window_date_end,
                                              latest=True),
                             AncQueryMetadata(base_name='trj',
                                              product='orb',
                                              start_date=public_release_window_date_start,
                                              end_date=public_release_window_date_end,
                                              latest=True),
                             AncQueryMetadata(base_name='MVN',
                                              product='SCLKSCET',
                                              latest=True),
                             # Don't provide JPL Radio files to public for now.
                             #                              AncQueryMetadata(base_name='202MA',
                             #                                               end_date=public_release_window_date_end,
                             #                                               latest=True),
                             SciQueryMetadata(instrument_list=['lpw', 'mag', 'ngi', 'pfp', 'sep', 'sta', 'swe', 'swi'],
                                              plan_list=['quicklook'],
                                              file_extension_list=[
                                                  'png', 'csv'],
                                              start_date=public_release_window_date_start,
                                              end_date=public_release_window_date_end),
                             SciQueryMetadata(instrument_list=['iuv'],
                                              file_extension_list=['png', 'jpg'],
                                              start_date=public_release_window_date_start,
                                              end_date=public_release_window_date_end),
                             SystemFileQueryMetadata(root_dir='/maven/data/sdc/trk',
                                                     base_name_pattern=trk_bundle_pattern,
                                                     start_date=public_release_window_date_start,
                                                     end_date=public_release_window_date_end,
                                                     get_date=lambda x: public_release_window_date_start,  # time doesn't matter for public release
                                                     child_directories=False),

                             ]


def populate_site(source_generators,
                  target_root_dir,
                  source_root_dir,
                  sym_link=True):
    '''Method used to populate the site using the provided source files
    Arguments:
        source_generators - The list of source generators used to populate the site
        target_root_dir - The root of the directory tree to populate
        source_root_dir - The root of the directory for the source science data
        sym_link - True - use symbolic links, False - full science file copies
    '''
    temp_released_list_file = source_root_dir + "/temp-released-site-list-" + str(datetime.datetime.now()) + '.txt'
    # create the file to ensure it's still created in case there are
    # no source generators
    open(temp_released_list_file, "a").close()
    for next_source_generator in source_generators:
        logger.info('Generating release data for %s', next_source_generator)
        for source_file in next_source_generator.generate():
            target_file = source_file

            if not os.path.isfile(source_file):
                logger.warning('Science file -%s- was not found!', source_file)
                continue

            anc_source_root_dir = os.path.join(source_root_dir, ANC_DIR)
            sci_source_root_dir = os.path.join(source_root_dir, SCI_DIR)
            sdc_source_root_dir = os.path.join(source_root_dir, SDC_DIR)

            if anc_source_root_dir in source_file:
                target_file = source_file.replace(
                    anc_source_root_dir, os.path.join(target_root_dir, ANC_DIR))
            elif sci_source_root_dir in source_file:
                target_file = source_file.replace(
                    sci_source_root_dir, os.path.join(target_root_dir, SCI_DIR))
            elif sdc_source_root_dir in source_file:
                target_file = source_file.replace(
                    sdc_source_root_dir, os.path.join(target_root_dir, SDC_DIR))
            else:
                raise Exception('File %s was not anc or sci' % target_file)
            # Does target dir exist?
            target_dir = os.path.dirname(target_file)
            if not os.path.isdir(target_dir):
                logger.info('Creating target directory %s', target_dir)
                os.makedirs(target_dir, mode=0o755)

            if os.path.isfile(target_file):
                logger.warning('%s target already exists!' % target_file)
                continue

            if sym_link:
                #logger.debug(
                #    'Creating symlink from %s to %s', target_file, source_file)
                try:
                    os.symlink(source_file, target_file)
                    os.chmod(target_file, 0o755)
                except FileExistsError as e:
                    logger.debug(str(e))
                    continue
            else:
                logger.debug('Copying from %s to %s', source_file, target_file)
                shutil.copy(source_file, target_file)
            with open(temp_released_list_file, 'a') as file:
                file.write(source_file + '\n')
    return temp_released_list_file


def build_site(root_dir,
               source_root_dir='/maven/data',
               sym_link=True,
               dry_run=False):
    '''Method to be used to build a MAVEN public site
    Arguments:
        root_dir - The public site root directory
        source_root_dir - The directory that contains all the maven data
        sym_link - True - use symbolic links, False - full file copies
        dry_run - If True, we don't modify the database or existing symlinks.  This is generally for testing only.   
    '''
    now = time_utilities.utc_now()

    logger.info('Building site @ %s from source root %s, use sym links %s', root_dir, source_root_dir, sym_link)

    # Create the root_dir if it doesn't exist
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)
    os.chmod(root_dir, 0o755)
    # Create a new directory for public files
    site_base_dir = os.path.join(
        root_dir, 'sites', 'site-%s' % now.strftime(directory_time_format))
    

    if dry_run:
        # Only create a list of the files to be released and then immediately return
        # Results should end up in /maven/data
        released_files_list = populate_site(source_generators=default_source_generators,
                                        source_root_dir=source_root_dir,
                                        target_root_dir=site_base_dir,
                                        sym_link=sym_link)
        return
    
    # Create ancillary directory 
    site_anc_dir = os.path.join(site_base_dir, ANC_DIR)
    if not os.path.exists(site_anc_dir):
        os.makedirs(site_anc_dir)
    # Create science directory
    site_sci_dir = os.path.join(site_base_dir, SCI_DIR)
    if not os.path.exists(site_sci_dir):
        os.makedirs(site_sci_dir)
    # Create SDC directory
    site_sdc_dir = os.path.join(site_base_dir, SDC_DIR)
    if not os.path.exists(site_sdc_dir):
        os.makedirs(site_sdc_dir)
    
    # Set names 
    data_orb_dir = os.path.join(source_root_dir, ORB_DIR)
    data_optg_dir = os.path.join(source_root_dir, OPTG_DIR)
    data_spice_dir = os.path.join(source_root_dir, SPICE_DIR)
    data_flare_dir = os.path.join(source_root_dir, EUV_FLARE_DIR)
    data_euv_l2b_dir = os.path.join(source_root_dir, EUV_L2B_DIR)
    data_euv_l3b_dir = os.path.join(source_root_dir, EUV_L3B_DIR)
    site_orb_dir = os.path.join(site_base_dir, ORB_DIR)
    site_optg_dir = os.path.join(site_base_dir, OPTG_DIR)
    site_spice_dir = os.path.join(site_base_dir, SPICE_DIR)
    site_flare_dir = os.path.join(site_base_dir, EUV_FLARE_DIR)
    site_euv_l2b_dir = os.path.join(site_base_dir, EUV_L2B_DIR)
    site_euv_l3b_dir = os.path.join(site_base_dir, EUV_L3B_DIR)
    site_symlink_anc_dir = os.path.join(root_dir, ANC_DIR)
    site_symlink_sci_dir = os.path.join(root_dir, SCI_DIR)
    site_symlink_misc_dir = os.path.join(root_dir, MISC_DIR)
    
    released_files_list = populate_site(source_generators=default_source_generators,
                                        source_root_dir=source_root_dir,
                                        target_root_dir=site_base_dir,
                                        sym_link=sym_link)
    
    # This exists as like a triple check to change the permissions
    command = "find " + site_base_dir + " -type d -exec chmod 755 {} ;"
    process = subprocess.run(command.split(), stdout=subprocess.PIPE)

    update_released(released_files_list)
    # If site sym links exists, swap them, if not, create them
    if os.path.islink(site_symlink_anc_dir):
        os.unlink(site_symlink_anc_dir)
    os.symlink(site_anc_dir, site_symlink_anc_dir)
    os.chmod(site_symlink_anc_dir, 0o755)
    if os.path.islink(site_symlink_sci_dir):
        os.unlink(site_symlink_sci_dir)
    os.symlink(site_sci_dir, site_symlink_sci_dir)
    os.chmod(site_symlink_sci_dir, 0o755)
    if os.path.islink(site_symlink_misc_dir):
        os.unlink(site_symlink_misc_dir)
    os.symlink(site_sdc_dir, site_symlink_misc_dir)
    os.chmod(site_symlink_misc_dir, 0o755)

    # Finally, release files not present on the PDS to the public
    if not os.path.exists(site_anc_dir):
        os.makedirs(site_anc_dir)
    if os.path.exists(data_orb_dir) and not os.path.exists(site_orb_dir):
        os.symlink(data_orb_dir, site_orb_dir)
        os.chmod(site_orb_dir, 0o755)
    if os.path.exists(data_optg_dir) and not os.path.exists(site_optg_dir):
        os.symlink(data_optg_dir, site_optg_dir)
        os.chmod(site_optg_dir, 0o755)
    if os.path.exists(data_spice_dir) and not os.path.exists(site_spice_dir):
        os.symlink(data_spice_dir, site_spice_dir)
        os.chmod(site_spice_dir, 0o755)
    if os.path.exists(data_flare_dir) and not os.path.exists(site_flare_dir):
        os.symlink(data_flare_dir, site_flare_dir)
        os.chmod(site_flare_dir, 0o755)
    if os.path.exists(data_euv_l2b_dir) and not os.path.exists(site_euv_l2b_dir):
        os.symlink(data_euv_l2b_dir, site_euv_l2b_dir)
        os.chmod(site_euv_l2b_dir, 0o755)
    if os.path.exists(data_euv_l3b_dir) and not os.path.exists(site_euv_l3b_dir):
        os.symlink(data_euv_l3b_dir, site_euv_l3b_dir)
        os.chmod(site_euv_l3b_dir, 0o755)
    


def clear_released():
    '''Method used to mark all science file metadata as not released'''
    ScienceFilesMetadata.query.filter(ScienceFilesMetadata.released).update({ScienceFilesMetadata.released: False})
    AncillaryFilesMetadata.query.filter(AncillaryFilesMetadata.released).update({AncillaryFilesMetadata.released: False})
    db_session.commit()


def update_released(released_file):
    '''Method used to mark files generated by the source_generators as 'released'
    Arguments:
        released_file - file path to list of released files
    '''
    # Clear released flags
    # clear_released()

    with open(released_file, 'r') as f:
        for source_file in f:
            source_file = source_file.rstrip('\n')
            path, base = os.path.split(source_file)
            path_parts = path.split(os.sep)
            meta = None

            if ANC_DIR in path_parts:  # Let's try ancillary tables first for files in .../anc/
                meta = AncillaryFilesMetadata.query.filter(AncillaryFilesMetadata.file_name == base).first()
                if meta is None:  # File not found in anc, try sci
                    meta = ScienceFilesMetadata.query.filter(ScienceFilesMetadata.file_name == base).first()
            else:  # Let's try science first for files in .../sci/
                meta = ScienceFilesMetadata.query.filter(ScienceFilesMetadata.file_name == base).first()
                if meta is None:
                    meta = AncillaryFilesMetadata.query.filter(AncillaryFilesMetadata.file_name == base).first()

            if meta is None:
                logger.warning("Unable to find %s in either the ancillary tables or the science tables!" % base)
                continue
            meta.released = True
        db_session.commit()


def check_site(root_dir):
    '''Generator used to report the symbolic links that no longer point to valid MAVEN science files
    Arguments:
        root_dir - The root directory to start the search for dead links
    Generates:
        A fully qualified path to a dead link.
    '''
    for dirpath, _, filenames in os.walk(root_dir, followlinks=True):
        for filename in [os.path.join(dirpath, filename) for filename in filenames]:
            if not os.path.isfile(filename):
                yield filename


def clean_site(root_dir):
    '''Method used to remove any dead symlinks
     Arguments:
        root_dir - The root directory to start the search for dead links
    '''
    for next_link in check_site(root_dir):
        logger.info('Removing dead symlink %s', next_link)
        os.unlink(next_link)


def remove_old_sites(root_dir, from_time=None):
    '''Method to be used to remove old public sites
    Arguments:
        root_dir:
        from_time: Public site directories created earlier (based on their site-YYYYMMDDTHHMMSS names)
                   then this time will be removed
    '''

    if not from_time:
        for dir_levels in os.readlink(os.path.join(root_dir, SCI_DIR)):
            m = site_sim_dir_pattern.match(dir_levels)
            if m:
                from_time = datetime.datetime.strptime(
                    directory_time_format, m.groups()[0])
                break

    if not from_time:
        logger.info(
            'Unable to find site for root_dir %s.  No sites will be removed!' % root_dir)
        return

    for next_dir in [next_dir for next_dir in os.listdir(root_dir) if os.path.isdir(next_dir)]:
        m = site_sim_dir_pattern.match(next_dir)
        if not m:
            logger.info('Directory %s is not a symlink', next_dir)
            continue

        dir_dt = datetime.datetime.strptime(
            directory_time_format, m.groups()[0]).replace(tzinfo=pytz.UTC)

        if dir_dt < from_time:
            logger.info(
                'Removing site dir %s', os.path.join(root_dir, next_dir))
            shutil.rmtree(os.path.join(root_dir, next_dir))


def walk_through_pds(url, ppi=False):
    ''' This method walks through the given URL, finds files that match the science files regex, and returns the list of files.
    Arguments:
        url: The URL where you would like to begin the walk.
    Returns: A list of files under that url that match maven science files
    '''

    start_year = public_release_window_date_start.year
    end_year = public_release_window_date_end.year
    released_files_list = []

    for year in range(start_year,end_year+1): # Loops through years

        #Determine which months to loop through
        if year is start_year:
            start_month = public_release_window_date_start.month
        else:
            start_month = 1
        months = range(start_month, 13)

        for month in months: # Loops through months
            page_date = datetime.datetime(year, month, 1, tzinfo=pytz.UTC)
            if (public_release_window_date_end - page_date).days < 0:  # Stop if exceeding release window
                # Return the found files
                return released_files_list

            
            year_string = str(year)
            month_string = str(month).zfill(2)
            _, end_day = monthrange(year, month)
            

            if ppi:
                # The following string looks super messy, but the point is to limit PPI API queries to certain months to avoid generating large amounts of files at a time.  
                # Without all of the odd characters, the string below looks reads something like this:
                # " AND start_date_time:[* TO 2015-11-01T23:59:59Z] AND stop_date_time:[2014-10-01T00:00:00Z TO *]"
                ppi_date_query_string = f"%20AND%20start_date_time:%5B*%20TO%20{year_string}-{month_string}-{end_day}T23:59:59Z%5D%20AND%20stop_date_time:%5B{year_string}-{month_string}-01T00:00:00Z%20TO%20*%5D"
                pds_data_url = url + ppi_date_query_string + config.ppi_suffix
                # A sample full URL that actually works is: 
                # https://pds-ppi.igpp.ucla.edu/metadex/product/select?q=collection_id:%22urn:nasa:pds:maven.euv.modelled:data.daily.spectra%22%20AND%20start_date_time:%5B*%20TO%202015-02-01T23:59:59Z%5D%20AND%20stop_date_time:%5B2014-10-11T00:00:00Z%20TO%20*%5D&rows=10000&indent=on&wt=json
            else:
                pds_data_url = os.path.join(url, year_string, month_string)

            # Grabs the data page and returns the page as a string
            try:
                page = urlopen(pds_data_url)
            except HTTPError:
                logger.warning("The page [%s] doesn't appear to exist.", pds_data_url)
                continue
            try:
                page_as_string = page.read().decode("utf-8")
            except Exception:
                logger.warning("The page [%s] could not be read.", pds_data_url)
                continue
            # Find all science files
            science_files_found = list(set(re.findall(config.pds_regex, page_as_string)))
            released_files_list.extend(science_files_found)

    return released_files_list
