# maven_db_log_handler.py
#
# Holds a class that logs messages
# to the MAVEN database.
#
# Mike Dorey  2013-07-23


import logging
from maven_database import db_session
from maven_database.models import MavenLog


class MavenDbLogHandler(logging.Handler):
    ''' A logging.Handler class that logs messages to the
    MAVEN database.
    '''

    def emit(self, record):
        '''Send a log message to the database.'''
        log = MavenLog(record.__dict__['name'],
                       record.__dict__['levelname'],
                       self.format(record))
        db_session.add(log)
        db_session.commit()
