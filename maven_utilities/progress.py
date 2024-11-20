# pylint: disable=E1101
'''
Created on Jun 30, 2015

@author: bstaley
'''
import logging
import sys
from enum import Enum

from maven_utilities import mail

logger = logging.getLogger('maven.maven_utilities.progress.log')


class ProgressHandler():
    '''Base class for Progress Handlers '''

    def handle(self, num_success, num_error, total):
        pass


class EmailProgressHandler(ProgressHandler):
    ''' EmailProgressHandler will send emails as progress milestones are reached.'''

    def __init__(self, prefix='', title=''):
        self.prefix = prefix
        self.title = title

    def handle(self, num_success, num_error, total):
        complete = num_success + num_error
        if total == 0:
            percentage = 100.0
        else:
            percentage = float(complete) / float(total) * 100.0

        mail.send_email(subject='{0} progress report for {1}'.format(self.title, self.prefix),
                        message=('The {0} process:\n\t'
                                 'Prefix: {1}\n\t'
                                 'Successful Files: {2}\n\t'
                                 'Failed Files: {3}\n\t'
                                 'Completed Files: {4}\n\t'
                                 'Total Files: {5}\n\t'
                                 '% Complete: {6:.2f}%').format(self.title, self.prefix,
                                                                num_success, num_error,
                                                                complete, total, percentage))


class BarProgressHandler(ProgressHandler):
    '''BarProgressHandler will display the progress as an ASCII progress bar (e.g. #####------) '''

    output = None
    prefix = None
    bar_len = 60

    def __init__(self, prefix='Complete', output=sys.stdout, bar_len=60):
        self.bar_len = bar_len
        self.output = output
        self.prefix = prefix

    def handle(self, num_success, num_error, total):
        complete = num_success + num_error
        if total == 0:
            self.output.write('{0} {1}'.format(self.prefix, '#' * self.bar_len))
            return
        percentage = float(complete) / float(total)
        num_complete_bars = self.bar_len * percentage
        self.output.write('{0} {1}{2}\r'.format(self.prefix,
                                                '#' * int(num_complete_bars),
                                                '-' * int(self.bar_len - num_complete_bars)))


class StandardProgressHandler(ProgressHandler):
    '''StandardProgressHandler will write the ASCII progress report to the provided data stream'''

    output = None
    prefix = None

    def __init__(self, prefix='Completed - ', output=sys.stdout):
        self.output = output
        self.prefix = prefix

    def handle(self, num_success, num_error, total):
        complete = num_success + num_error
        if total == 0:
            self.output.write('{0} ({1}/{2}) {3}%\n'.format(self.prefix, complete, total, 1.0))
            return
        percentage = float(complete) / float(total) * 100.0
        self.output.write(('{0} ({1}/{2}) {3:.2f}% '
                           'Error: {4} Successful: {5}\n').format(self.prefix, complete,
                                                                  total, percentage,
                                                                  num_error, num_success))


class Progress():
    '''Progress will manage the ongoing progress of some task.  As the task finishes units of work, Progress will
    determine if registered ProgressHandlers need to be fired. '''

    class STATUS(Enum):
        IN_PROGRESS = 1
        COMPLETE = 2
        ERROR = -1

    def __init__(self, units_of_work, fire_at_init=None):
        '''Method used to initialize the Progress with the units of work
        Arguments:
            units_of_work - A list of objects that will be used to status against
            fire_at_init - A list of handlers to be fired when the Progress object is initialized.
        '''
        self.status = {}
        self.every_handlers = []
        self.cadence_handlers = {}

        for unit in units_of_work:
            self.status[unit] = self.STATUS.IN_PROGRESS

        if fire_at_init is not None:
            for h in fire_at_init:
                h.handle(num_success=0, num_error=0, total=len(self.status))

    def add_handler(self, handler, cadence=None):
        '''Method used to register a ProgressHandler
        Arguments:
            handler - The ProgressHandler to be registered
            cadence - The cadence at which the ProgressHandler is to be fired.  If cadence = None, the ProgressHandler will be fired on every update
        '''
        if cadence is None:
            logger.debug("cadence is %s and will be fired on every update", cadence)
            self.every_handlers.append(handler)
        else:
            logger.debug("cadence is not None. cadence of ProgressHandler is %s", cadence)
            self.cadence_handlers[handler] = [float(i + 1) * cadence for i in range(0, int(1.0 / cadence))]

    def handle_update(self):
        '''Method used to determine if there are outstanding ProgressHandlers to fire'''
        complete = self.get_total_percentage()
        for h in self.every_handlers:
            h.handle(self.get_status_count(self.STATUS.COMPLETE),
                     self.get_status_count(self.STATUS.ERROR),
                     len(self.status))
        for h in self.cadence_handlers:
            for cadence in [cadence for cadence in self.cadence_handlers[h] if cadence <= complete]:
                h.handle(self.get_status_count(self.STATUS.COMPLETE),
                         self.get_status_count(self.STATUS.ERROR),
                         len(self.status))
                break  # only fire handler once
            # remove the fired handlers
            self.cadence_handlers[h] = [cadence for cadence in self.cadence_handlers[h] if cadence > complete]

    def update_status(self, unit_of_work, status):
        '''Method used to change the status of a unit of work
        Arguments:
            unit_of_work - The unit_of_work to be statused
            status - The new unit of work status
        '''
        if unit_of_work in self.status:
            logger.debug("unit_of_work added to dictionary as {%s:%s}", unit_of_work, status)
            self.status[unit_of_work] = status.value
        else:
            logger.warning("Unit %s is not being statused", unit_of_work)
        self.handle_update()

    def complete_unit(self, unit_of_work):
        '''Method used to mark a unit as COMPLETE'''
        self.update_status(unit_of_work, self.STATUS.COMPLETE)

    def error_unit(self, unit_of_work):
        '''Method used to mark a unit as ERROR'''
        self.update_status(unit_of_work, self.STATUS.ERROR)

    def get_status_count(self, status):
        '''Method used to get the current unit count for the provided status
        Arguments:
            status - Count only the units that have this status
        '''
        return sum(1 for i in self.status if self.status[i] == status)

    def get_not_status_count(self, status):
        '''Method used to get the current unit count for all units that don't have the provided status
        Arguments:
            status - Count only the units that don't have this status
        '''
        return sum(1 for i in self.status if self.status[i] != status)

    def get_complete_percentage(self):
        '''Method used to get the COMPLETE %'''
        length = float(len(self.status))
        if length == 0:
            logger.debug("length is %d converted to 1.0", length)
            return 1.0
        return float(self.get_status_count(self.STATUS.COMPLETE) / length)

    def get_error_percentage(self):
        '''Method used to get the ERROR %'''
        length = float(len(self.status))
        if length == 0:
            logger.debug("length is %d converted to 1.0", length)
            return 1.0
        return self.get_status_count(self.STATUS.ERROR) / length

    def get_total_percentage(self):
        '''Method used to get the total (everything but IN_PROGRESS) %'''
        length = float(len(self.status))
        if length == 0:
            logger.debug("length is %d converted to 1.0", length)
            return 1.0
        return self.get_not_status_count(self.STATUS.IN_PROGRESS) / length
