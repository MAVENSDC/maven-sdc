'''
Module used to track the progress of an ongoing PDS archive and report the ongoing progress

Created on Jun 17, 2015

@author: bstaley
'''
import os
import sys
from maven_utilities.progress import Progress, StandardProgressHandler
from maven_status import MAVEN_SDC_COMPONENT
from maven_status.progress import StatusProgressHandler


class ArchiveProgress(Progress):
    '''PDS Archive Progress reporter'''

    def __init__(self, files_to_status, prefix):
        super(ArchiveProgress, self).__init__(
            [os.path.basename(f) for f in files_to_status],
            fire_at_init=[StatusProgressHandler(component=MAVEN_SDC_COMPONENT.PDS_ARCHIVER, product=prefix)]
        )

        # setup PDS Archive status handlers
        self.add_handler(StandardProgressHandler(prefix=('{0} % Complete').format(prefix),
                                                 output=sys.stderr))  # send to stderr so nohup doesn't consume it)

    def update_status(self, unit_of_work, status):
        super(ArchiveProgress, self).update_status(os.path.basename(unit_of_work), status)
