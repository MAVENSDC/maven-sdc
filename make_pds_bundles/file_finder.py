'''
Created on Aug 22, 2016

@author: bstaley
'''
import os
import re
import datetime
import operator
import logging
from sqlalchemy import or_

from . import utilities
from maven_database.models import ScienceFilesMetadata
from maven_utilities import time_utilities

logger = logging.getLogger('maven.make_pds_bundles.make_pds_bundles.file_finder.log')


class InventoryFileFinder():
    '''Generator class used to find PDS destined data files based on a PDS inventory file of the form:

    P,urn:nasa:pds:maven.iuvs.calibrated:disk:mvn_iuv_l1b_apoapse-orbit01274-fuv_20150527t050045::4.0
    P,urn:nasa:pds:maven.iuvs.calibrated:disk:mvn_iuv_l1b_apoapse-orbit01274-fuv_20150527t052719::4.0
    ....
    '''

    # PDS entry pattern
    inv_pattern = (r'^P,urn:nasa:pds:'
                   '(?P<{0}>[^:]+):'  # urn
                   '(?P<{1}>[^:]+):'  # level
                   '(?P<{2}>[^:]+)::'  # base name
                   '(?P<{3}>[\d]+)\.(?P<{4}>[\d]+)'  # version/revision
                   ).format('urn',
                            'level',
                            'base',
                            'version',
                            'revision')
    inv_regex = re.compile(inv_pattern)
    
    acc_inv_pattern = (r'^P,urn:nasa:pds:'
                       '(?P<{0}>[^:]+):'  # urn
                       '(?P<{1}>[^:]+):'  # level
                       '(?P<{2}>[^:]+)_' # base name
                       'v(?P<{3}>[\d]+)_r(?P<{4}>[\d]+)'  # version/revision
                       ).format('urn',
                                'level',
                                'base',
                                'version',
                                'revision')
                                
    acc_inv_regex = re.compile(acc_inv_pattern)
    

    def __init__(self,
                 inv_file,
                 results_from_dt=datetime.datetime.min,
                 results_to_dt=datetime.datetime.max,
                 results_extensions=None,
                 results_not_extensions=None,
                 results_version=None,
                 results_revision=None,
                 missing_file_handler=None,
                 uprev_inv_file=False):
        '''Constructor
        Arguments:
            inv_file : The inventory file to use
            results_from_dt : Timetag start filter
            results_to_dt: Timetag end filter
            results_extension : Inventory files extension filter (only applied to files found in the inventory)
            results_version : Inventory version filter (only applied to files found in the inventory)
            results_revision : Inventory revision filter (only applied to files found in the inventory)
            missing_file_handler : f(x) for missing files
            uprev_inv_file : If true, increment the revision of the files found in the inventory by 1
        '''

        self.inv_file = inv_file
        self.uprev_inv_file = uprev_inv_file
        self.results_version = results_version
        self.results_revision = results_revision
        self.results_extensions = results_extensions if results_extensions else []
        self.results_not_extenstions = results_not_extensions if results_not_extensions else []
        self.from_dt = results_from_dt
        self.to_dt = results_to_dt
        self.missing_file_handler = missing_file_handler if missing_file_handler else lambda x: None

    # pylint: disable=R0915
    def generate(self):
        '''Method used to generate Science Data Files
        Yields:
            Fully qualified paths to Science Data Files.
        '''

        def parse_inventory_line(line):
            '''Helper method used to parse a Inventory line
            Arguments:
                line : The next line to parse
            Returns:
                (base_name,version,revision)
            '''
            
            m = self.inv_regex.match(line)
            if m is None:
                m = self.acc_inv_regex.match(line)
            
            if m is None:
                raise ValueError("{0} doesn't meet regex {1}".format(line, self.inv_pattern))

            try:
                base_name = re.sub(r'(?P<YYYYMMDD>[\d]{8})t(?P<HHMMSS>[\d]{6})',
                                   r'\g<YYYYMMDD>T\g<HHMMSS>', m.group('base'))

                time_string = re.search(r'(?P<YYYYMMDD>[\d]{8})T(?P<HHMMSS>[\d]{6})',
                                       base_name)

                start_date = time_utilities.to_utc_tz(datetime.datetime.strptime(time_string.group(0), '%Y%m%dt%H%M%S'))
            except:
                time_string = re.search(r'(?P<YYYYMMDD>[\d]{8})', base_name)
                start_date = time_utilities.to_utc_tz(datetime.datetime.strptime(time_string.group(0), '%Y%m%d'))

            version = int(m.group('version'))
            revision = int(m.group('revision'))
            
            if self.uprev_inv_file:
                revision = revision+1

            return base_name, version, revision, start_date

        def compare(sfmd, inv_tuple, ver, rev):
            '''Method used to compare a ScienceFileMetadata object and a inventory tuple
            Arguments:
                sfmd - The ScienceFileMetadata object to compare
                ivn_tuple - The inventory tuple (base_name,(version,revision))
                ver - Version override
                rev - Revision override
            Returns:
                -1 if sfmd < inv_tuple, 1 if sfmd > inv_tuple 0 if sfmd == inv_tuple
            '''
            sfmd_base_name = re.sub(r'\..+$', r'', sfmd.file_root)
            inv_ver_rev = (ver if ver is not None else inv_tuple[1][0],
                           rev if rev is not None else inv_tuple[1][1])

            def _cmp(a, b):
                return (a > b) - (a < b)

            return _cmp((sfmd_base_name, (sfmd.version, sfmd.revision)),
                       (inv_tuple[0], inv_ver_rev))

        logger.info("Processing inventory file :%s", self.inv_file)

        if not os.path.isfile(self.inv_file):
            err_msg = "{0} wasn't found on the file system!".format(self.inv_file)
            logger.error(err_msg)
            return

        inv_entries = []
        # ingest inventory file into
        with open(self.inv_file, 'r') as inv_file:

            while True:
                next_inv_line = inv_file.readline()

                if not next_inv_line:
                    break

                try:
                    next_inv = parse_inventory_line(next_inv_line)
                except ValueError:
                    logger.warning("The line [%s] doesn't appear to be an inventory entry.  Skipping inventory file :%s ", next_inv_line, self.inv_file)
                    continue

                base_name, version, revision, start_date = next_inv

                inv_entries.append((base_name, (version, revision), start_date))
        inv_entries.sort()
        if len(inv_entries) == 0:
            logger.warning('No inventory entries found')
            return
        # query db to find file existence/location
        query = ScienceFilesMetadata.query.filter(operator.ge(ScienceFilesMetadata.file_root, inv_entries[0][0]))

        query = query.filter(operator.le(ScienceFilesMetadata.file_root, inv_entries[-1][0] + 'Z'))  # the Z is a bit of a hack to find the last entry (db file_root has .<ext>)
        ext_filter = []
        for ext in self.results_extensions:
            ext_filter.append(operator.eq(ScienceFilesMetadata.file_extension, ext))
        query = query.filter(or_(*ext_filter))
        not_ext_filter = []
        for ext in self.results_not_extenstions:
            not_ext_filter.append(operator.ne(ScienceFilesMetadata.file_extension, ext))
        query = query.filter(or_(*not_ext_filter))
        if self.results_version:
            query = query.filter(ScienceFilesMetadata.version == self.results_version)
        query = query.filter(ScienceFilesMetadata.timetag >= self.from_dt,
                                ScienceFilesMetadata.timetag < self.to_dt)
        # order by families alphabetically descending version/revision
        query = query.order_by(ScienceFilesMetadata.file_root, ScienceFilesMetadata.absolute_version)
        next_inv_entry = inv_entries.pop(0)

        missing_inv = []
        
        # Verify that there is at least one result in the query, otherwise we won't enter the loop below (and therefore no errors will pop up).   
        test_query = query.first()
        if not test_query:
            logger.warning("There were no files on the SDC that matched this data type and time range!  Make sure that files were actually delivered.")

        try:
            for next_sfmd in query.yield_per(20):
                if compare(next_sfmd, next_inv_entry, self.results_version, self.results_revision) < 0:  # DB is behind inventory
                    continue  # process next sfmd

                while compare(next_sfmd, next_inv_entry, self.results_version, self.results_revision) > 0:  # DB is ahead of inventory
                    if next_inv_entry[2] > self.from_dt and next_inv_entry[2] < self.to_dt: # Determine if file is in the time range we're looking for
                        logger.warning("The inventory entry %s wasn't found in the SDC!", next_inv_entry[0])
                        missing_inv.append((next_inv_entry))
                    next_inv_entry = inv_entries.pop(0)

                if compare(next_sfmd, next_inv_entry, self.results_version, self.results_revision) == 0:
                    yield os.path.join(next_sfmd.directory_path, next_sfmd.file_name)
                    next_inv_entry = inv_entries.pop(0)

            # gather remaining inv entries
            for _next in inv_entries:
                missing_inv.append(_next)
            

        except IndexError:  # reached end of next_inv_entry list
            pass
        finally:
            for _next in missing_inv:
                self.missing_file_handler(_next)
            if hasattr(query, 'session'):
                query.session.close()




class ScienceQueryFileFinder():
    '''Generator class used to find PDS destined data files based on science_files_metadata table queries'''

    def __init__(self,
                 instrument_list=None,
                 level_list=None,
                 extension_list=None,
                 plan_list=None,
                 file_name=None,
                 from_dt=datetime.datetime.min,
                 to_dt=datetime.datetime.max,
                 version=None,
                 revision=None):
        '''Constructor
        Arguments:
            instrument_list : Instrument filters
            level_list : Level filters
            extension_list : File extension filters
            plan_list : Plan filters
            from_dt : Timetag start filter
            to_dt: Timetag end filter
            version : Version filter
            revision : Revision filter
        '''
        self.science_files_query = utilities.query_for_science_files(instrument_list=instrument_list,
                                                                     plan_list=plan_list,
                                                                     level_list=level_list,
                                                                     extension_list=extension_list,
                                                                     from_dt=from_dt,
                                                                     to_dt=to_dt,
                                                                     file_name=file_name,
                                                                     version=version,
                                                                     revision=revision,
                                                                     stream_results=True,
                                                                     latest=True
                                                                     )

    def generate(self):
        '''Method used to generate Science Data Files
        Yields:
            Fully qualified paths to Science Data Files.
        '''
        for next_science_file_metadata in self.science_files_query:
            yield os.path.join(next_science_file_metadata.directory_path, next_science_file_metadata.file_name)
