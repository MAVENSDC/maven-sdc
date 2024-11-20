'''
Module used to track the progress of an ongoing In Situ Kp File Ingest and report the ongoing progress

Created on Jul 2, 2015

@author: bussell
'''
import os
import sys
from maven_utilities.progress import Progress, StandardProgressHandler
from maven_status import MAVEN_SDC_COMPONENT, MAVEN_SDC_EVENTS
from maven_status.progress import StatusProgressHandler


class InSituIngestProgress(Progress):
    '''In Situ Kp Ingest Progress reporter'''

    def __init__(self, files_to_status, prefix):
        super(InSituIngestProgress, self).__init__(
            [os.path.basename(f) for f in files_to_status],
            fire_at_init=[StatusProgressHandler(component=MAVEN_SDC_COMPONENT.KP_INGESTER)]
        )

        # setup In Situ Ingest status handlers
        self.add_handler(StandardProgressHandler(prefix='{0} % Complete'.format(prefix),
                                                 output=sys.stderr))  # send to stderr so nohup doesn't consume it
        self.add_handler(StatusProgressHandler(component=MAVEN_SDC_COMPONENT.KP_INGESTER),
                         cadence=0.25)

    def update_status(self, unit_of_work, status):
        super(InSituIngestProgress, self).update_status(os.path.basename(unit_of_work), status)

    def callback_function(self, args):
        self.update_status(args[0], MAVEN_SDC_EVENTS(args[1]))
