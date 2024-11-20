'''Submit a request to get inventory from a glacier archive vault.

Outputs a job_id, which can then be passed to the next step.

Created on Sep 29, 2014

@author: Kim Kokkonen
'''
import sys
import time
from datetime import datetime
from aws_archiving import utilities
import logging
from optparse import OptionParser

logger = logging.getLogger('')

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-r', '--aws-region', type="string", dest="aws_region", default="us-west-2",
                      help="AWS region where the Glacier vault lives, default is us-west-2")

    options, args = parser.parse_args()
    if len(args) < 1 or len(args) > 3:
        print("Usage: %s <vault name> [<start_date> [<end_date>]]" % sys.argv[0])
        print("Date format yyyy-mm-dd")
        print("If start_date missing, requests all dates")
        print("If end_date missing, requests everything from start_date to present")
        sys.exit(-1)
    else:
        try:
            vault_name = args[0]
            start_date = None
            end_date = None
            if len(args) >= 2:
                ds = time.strptime(args[1], '%Y-%m-%d')
                start_date = datetime(ds.tm_year, ds.tm_mon, ds.tm_mday, tzinfo=utilities.UTC())
            if len(args) == 3:
                ds = time.strptime(args[2], '%Y-%m-%d')
                end_date = datetime(ds.tm_year, ds.tm_mon, ds.tm_mday, tzinfo=utilities.UTC())
            utilities.submit_inventory_request(options.aws_region, vault_name, start_date, end_date)
        except Exception as e:
            logger.exception("Exception submitting request", exc_info=e)
