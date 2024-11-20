'''Add a single tar file's contents to the local sqlite inventory.
Used when a backup job fails after uploading a tarball to glacier.

Created on Oct 11, 2014

@author: Kim Kokkonen
'''
import sys
from optparse import OptionParser
from aws_archiving import utilities

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-r', '--aws-region', type="string", dest="aws_region", default="us-west-2",
                      help="AWS region where the Glacier vault lives, default is us-west-2")
    options, args = parser.parse_args()
    if len(args) != 3:
        print("Usage: %s <vault name> <tarball filename> <glacier archive id>" % sys.argv[0])
        sys.exit(-1)
    else:
        vault_name = args[0]
        tarball_filename = args[1]
        glacier_archive_id = args[2]
        aws_region = options.aws_region
        utilities.add_archive_to_inventory(tarball_filename, glacier_archive_id, aws_region, vault_name)
