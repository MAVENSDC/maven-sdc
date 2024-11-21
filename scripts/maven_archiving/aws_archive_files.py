'''
A script that archives the files in a
directory tree  using Amazon Web Services
Glacier for the cold storage.

Intended to run in cron.
Called by the shell script archive-files-in-glacier.sh,
which defines mission-specific arguments.

@author: Mike Dorey
@author: Kim Kokkonen
@since: 2013-11-25
'''
#

import sys
from aws_archiving import utilities

if __name__ == "__main__":
    utilities.main(sys.argv)
