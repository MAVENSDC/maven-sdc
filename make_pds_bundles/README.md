Creates bundled tar files to be provided to the PDS


This module is designed to:
	1. Identify all files in a given directory
	2. Select a subset of files that matches a regular expression 
	   that is consistent with the definitions in the SIS document
	   for the given instrument
	3. Create a Manifest Table for that subset of data
	4. Create a tarball of the selected files
	5. Create a checksum manifest displaying md5 checksums for each file in bundle


1. make_pds_bundles: the main driver code, called with two date string 
   arguments.  I.e., For a bundle from 01-Nov-2014 to 28-Feb-2015, issue the
   command, "make_pds_bundles '20141101' '20150228'".  This routine parses 
   the input arguments, then the input dates, and then iterates in a loop 
   over all MAVEn instruments. This code will place all products into the 
   /maven/data/arc/ directory on sdc-task1.
   It will require sub-directories for each instrument.
   This routine has the primary definitions:

  a. input_dates: We will be bundling all data from a semi-arbitrary start
     date to a semi-arbitrary end date.  This brief definition simply 
     takes the two strings that were entered at the command line, converts
     them to date types in python, and assigns the earlier date as 'start'
     and later as 'end' (so it won't matter in which order the dates are
     entered on the command line: I.e., be careful with your date entries
     because the code will happily search for all data starting from 214 AD
     if you happen to mistype '20141101' as '02141101').

  b. mvn_ymd_to_doy: This is needed because the Ancillary Engineering Data
     comes to use with a different naming scheme for the date stamp.  I.e., 
     YYDDD, where YY is the two digit year (i.e., year mod 100), and 
     DDD is the day of year starting from Jan 1 as 001.

  c. get_valid_files: starting from the instrument level directory (e.g., 
     /maven/data/sci/swi/), descend through all lower directories (path, year,
     month, level), and identify all files that match both the given date
     range for the bundle and the expected regular expression for the file
     names that match what the instrument team has promised to provide to 
     the PDS.  The ancillary data must be treated differently here because
     of the different date stamp format.  Note, this is only checking for
     validity.  So, if there are ten versions of a particular data file, they
     will *all* be captured here.

  d. get_bundle_files: from the list of valid files, check the version and
     revision numbers for each product.  Select only the last version and
     revision for each product, and place that filename in the bundle list.

  e. print_transfer_manifest: This creates the transfer manifest for the given
     instrument.  It strips the filename suffix from each data product, 
     replacing it with an 'xml' suffix (because the transfer manifest maps
     product IDs to data labels).  The LIDVID are created from a combination
     of information stored in the instrument dictionary (see below), and a 
     parsing the filename to determine the version and revision number.

  f. print_checksum_manifest: This creates the checksum manifest, mapping the
     calculated md5 checksum for each file to the filename.

  g. get_md5_checksum: This performs the calculation of the md5 checksum. 

  h. tar_bundle_files: this cycles through each file that has been identified 
     to be placed in the PDS bundle for the current instrument, appending it
     to a tar file, and then gzipping the tarfile.  It then also renames the
     gzipped tarfile (changes the suffix from .tar.gz to .tgz) to be consistent
     with current convention.



