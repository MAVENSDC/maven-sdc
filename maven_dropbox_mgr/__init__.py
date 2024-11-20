import datetime
import pytz
from datetime import tzinfo
from maven_utilities import time_utilities

# Time used to determine when the maven_dropbox_mgr package was initialized.
# This is used to create dated directories for a set of dropbox items
init_time = time_utilities.utc_now()
