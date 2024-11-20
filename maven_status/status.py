'''
Created on Aug 4, 2016

@author: bstaley
'''

import uuid
import os
import logging

from maven_database.models import MavenStatus
from maven_utilities import time_utilities, constants
from maven_database import db_session
from . import global_component_id

env = os.environ.get(constants.python_env, None)
current_job_id = uuid.uuid4().int  # allocate a new job ID when package is imported

logger = logging.getLogger('maven.maven_status.status.log')


def add_status(event_id,
               component_id=None,
               job_id=None,
               summary=None,
               description=None,
               timetag=None):
    '''Method used to add an SDC job status
    Arguments:
        component_id: The maven_status:MAVEN_SDC_COMPONENT for this status
        event_id: The maven_status:MAVEN_SDC_EVENTS for this status
        summary: A short, descriptive string of the provided status
        description: A longer, more detailed string of the provided status
        timetag: The UTC time of the status
    '''
    if (component_id == None) and (len(global_component_id) == 0):
        raise ValueError('Either component_id or global_component_id must not be None')
    component_id = component_id  or global_component_id[-1]

    if job_id is None:
        job_id = current_job_id

    new_status = MavenStatus(component_id=component_id.name,
                             event_id=event_id.name,
                             job_id=job_id,
                             summary=str(summary),
                             description=description,
                             timetag=timetag if timetag else time_utilities.utc_now())
    if env:
        db_session.add(new_status)
        db_session.commit()
    else:
        logger.info(new_status)


def add_exception_status(event_id,
                         component_id=None,
                         job_id=None,
                         summary=None,
                         timetag=None):
    '''Method used to add an SDC job status extracting the most recent exception
    information
    Arguments:
        component_id: The maven_status:MAVEN_SDC_COMPONENT for this status
        event_id: The maven_status:MAVEN_SDC_EVENTS for this status
        summary: A short, descriptive string of the provided status
        timetag: The UTC time of the status
    '''
    if (component_id == None) and (len(global_component_id) == 0):
        raise ValueError('Either component_id or global_component_id must not be None')
    component_id = component_id  or global_component_id[-1]

    import traceback
    return add_status(event_id=event_id,
                      component_id=component_id,
                      job_id=job_id,
                      summary=summary,
                      description=traceback.format_exc(),
                      timetag=timetag)
