'''
Created on Nov 19, 2015

@author: cosc3564
'''
import os
import datetime
import pytz

# Environment Variables
python_env = 'MAVEN_PYTHON_ENV'
MAVEN_DB_TABLE_SUFFIX = 'MAVEN_DB_TABLE_SUFFIX'

# Filemagic Identification
magic_gzip_type = 'application/x-gzip'
magic_id_byte_size = 4

# Rotating File Handler
log_maxByte_size = 1000000000
log_backup_count = 1000

MAVEN_MISSION_START = datetime.datetime(2014, 9, 21).replace(tzinfo=pytz.UTC)  # MAVEN orbit insertion date