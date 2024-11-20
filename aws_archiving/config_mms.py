'''Constants used for archiving.

@author: Mike Dorey
'''
import os

env = os.environ.get("PYTHON_ENV", "testing")

if env == "testing":
    db_uri = ":memory:"
    root_logging_directory = '/tmp/aws_test_logs '
elif env == "production":
    db_uri = "mms-glacier-inventory.sqlite3"
    root_logging_directory = 'glacier-logs'

mission_name = 'mission'

email_sender = 'mmspro@lasp.colorado.edu'
email_recipients = ['mmssdc@lasp.colorado.edu']

#tarball_directory = os.path.join(os.environ['HOME'], 'glacier-archive-tarballs')
tarball_directory = '/mms_backups'

# if use_upload == False, the directory where tarballs
# are moved after they are completely built and logged in the db
completed_tarball_directory = os.path.join(tarball_directory, 'completed')

# directory where tarballs are moved after successful upload
uploaded_tarball_directory = os.path.join(tarball_directory, 'uploaded')

# if uploads are disabled, this prefix is added to the tarball filename
# and stored in the database as the "glacier_archive_id"
amazon_s3_prefix = 's3://mms_backups/'

# max raw file size for a single tar file, currently 4GB
max_tar_size = 4 * 1024 * 1024 * 1024

# set to True to gzip the tarballs
use_gzip = False

# set to True to upload the tarballs to Glacier
# otherwise a separate mechanism (Snowball, AWS CLI) is used to upload
use_upload = False

# set to True to delete tarballs after successful upload
# otherwise, they are moved to uploaded_tarball_directory
delete_tarball = False
