'''Submit a request to download all the archives specified by an inventory file.

Created on Sep 29, 2014

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
    if len(args) != 3:
        print("Usage: %s <vault name> <restore_root_directory> <inventory_file>" % sys.argv[0])
        sys.exit(-1)
    else:
        try:
            vault_name = args[0]
            restore_root_directory = args[1]
            inventory_file = args[2]
            status, job_ids = utilities.submit_download_request(options.aws_region, vault_name, restore_root_directory,
                                              inventory_file)
            sys.exit(status)
        except Exception as e:
            logger.exception("Exception submitting request", exc_info=e)
