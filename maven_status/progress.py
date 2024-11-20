'''
Created on Aug 19, 2016

@author: bstaley
'''
from maven_utilities.progress import ProgressHandler
from . import MAVEN_SDC_EVENTS
from . import status


class StatusProgressHandler(ProgressHandler):
    '''StatusProgressHandler will provide status updates via maven_status'''

    def __init__(self, component, product=None):
        self.component = component
        self.product = product if product else ""

    def handle(self, num_success, num_error, total):
        complete = num_success + num_error
        if total == 0:
            percentage = 100.0
        else:
            percentage = float(complete) / float(total) * 100.0

        status.add_status(event_id=MAVEN_SDC_EVENTS.PROGRESS,
                          component_id=self.component,
                          summary='{0} - {1} % Complete {2:.2f}%'.format(self.component.name, self.product, percentage),
                          description='Successful Files: {0}\n\t'
                                      'Failed Files: {1}\n\t'
                                      'Completed Files: {2}\n\t'
                                      'Total Files: {3}\n\t'
                                      '% Complete: {4:.2f}%'.format(num_success, num_error,
                                                                    complete, total, percentage))
