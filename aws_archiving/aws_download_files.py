'''Checks all open jobs on the vault. As long as any jobs of type archive-retrieval
are complete, it downloads the archive to a directory based on restore_root_directory
and a filename based on the original tarball name. If no jobs exist or an error
occurs, it returns a nonzero exit status. If jobs exist but are not complete, it returns an exit
status of 0.

Created on Sep 30, 2014

@author: Kim Kokkonen
'''
import sys
from aws_archiving import utilities
import logging
from optparse import OptionParser

logger = logging.getLogger('')


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-r', '--aws-region', type="string", dest="aws_region", default="us-west-2",
                      help="AWS region where the Glacier vault lives, default is us-west-2")

    options, args = parser.parse_args()
    if len(args) != 2:
        print("Usage: %s <vault name> <restore_root_directory>" % sys.argv[0])
        sys.exit(-1)
    else:
        try:
            vault_name = args[0]
            restore_root_directory = args[1]
            aws_region = options.aws_region
            status = utilities.download_files(aws_region, vault_name, 
                           restore_root_directory)
            sys.exit(status)
            
        except Exception as e:
            logger.exception("Exception downloading files", exc_info=e)
