'''
Created on Aug 11, 2016

@author: bstaley
'''
import sys
from io import StringIO
import logging

import maven_status
from maven_utilities.singleton import SingleInstance
from maven_utilities import maven_log
from maven_status import status as status_api


class DebugJob():
    '''
    A Job that doesn't capture stdout.
    '''

    def __init__(self, component_id, config_logging=None, singleton=True, flavor_id=""):
        '''
        Constructor used to setup logging and singleton semantics
        Arguments:
            component_id: The maven_status:MAVEN_SDC_COMPONENT for this job
            config_logging: The logging configuration name or none for env logging setup
            singleton: Boolean indicating if this job should be ran as a singleton
            flavor_id: The singleton flavor_id
        '''
        self.component_id = component_id

        maven_log.config_logging(config_logging)

        if singleton:
            self.singleton_instance = SingleInstance(flavor_id=flavor_id)

    def __enter__(self):
        '''For entering the context manager'''
        status_api.global_component_id.append(self.component_id)
        return self

    def __exit__(self, *args):
        '''For exiting the context manager'''
        status_api.global_component_id.pop()

    # pylint: disable=W0613
    # Disables the unused kwargs arguments in run
    def run(self,
            proc,
            proc_args=None,
            **kwargs):
        ''' The main entry point for a status job.  This method will call the provided
            proc as the job entry point.
        Arguments:
            proc: The function that is the entry point of the job
            proc_args: A dictionary of arguments to be provided to proc
            propagate_exceptions: If true, exceptions are propagated
        '''
        try:
            proc(**proc_args)
        finally:
            logging.shutdown()


class StatusJob():
    '''
    StatusJob is a class wrapper for SDC jobs.  The StatusJob wrapper does many of the
    common setup tasks an SDC job would otherwise perform.  
    '''

    def __init__(self, component_id, config_logging=None, singleton=True, flavor_id=""):
        '''
        Constructor used to setup logging and singleton semantics
        Arguments:
            component_id: The maven_status:MAVEN_SDC_COMPONENT for this job
            config_logging: The logging configuration name or none for env logging setup
            singleton: Boolean indicating if this job should be ran as a singleton
            flavor_id: The singleton flavor_id
        '''
        self.component_id = component_id

        maven_log.config_logging(config_logging)

        if singleton:
            self.singleton_instance = SingleInstance(flavor_id=flavor_id)

    def __enter__(self):
        '''For entering the context manager'''
        status_api.global_component_id.append(self.component_id)
        return self

    def __exit__(self, *args):
        '''For exiting the context manager'''
        status_api.global_component_id.pop()

    # pylint: disable=W0613
    # Disables the unused kwargs arguments in run
    def run(self,
            proc,
            proc_args=None,
            **kwargs):
        ''' The main entry point for a status job.  This method will call the provided
            proc as the job entry point.
        Arguments:
            proc: The function that is the entry point of the job
            proc_args: A dictionary of arguments to be provided to proc
            propagate_exceptions: If true, exceptions are propagated
        '''

        proc_name = proc.__name__
        proc_args = proc_args if proc_args else {}
        try:
            status_api.add_status(event_id=maven_status.MAVEN_SDC_EVENTS.START,
                                  component_id=self.component_id,
                                  summary='{0} Started'.format(proc_name),
                                  description='{0}'.format(proc_args))
            # run job
            proc(**proc_args)
        except Exception as e:
            status_api.add_exception_status(component_id=self.component_id,
                                            event_id=maven_status.MAVEN_SDC_EVENTS.FAIL,
                                            summary='{0} Failed!'.format(proc_name))
            if 'propagate_exceptions' in kwargs and kwargs['propagate_exceptions'] == True:
                raise e
        else:
            status_api.add_status(event_id=maven_status.MAVEN_SDC_EVENTS.SUCCESS,
                                  component_id=self.component_id,
                                  summary='{0} Successfully Finished'.format(proc_name))
        finally:
            logging.shutdown()


class StatusCronJob(StatusJob):
    '''
    StatusCronJob is a class wrapper for SDC jobs.  The StatusCronJob wrapper intercepts
    all stdout (stderr is left intact) and provides the stdout as a job status
    '''

    STD_OUT_LEN = 256

    def __init__(self, component_id, config_logging=None, singleton=True, flavor_id=""):
        super(StatusCronJob, self).__init__(component_id, config_logging, singleton, flavor_id)
        '''
        Constructor used to setup logging and singleton semantics
        Arguments:
            component_id: The maven_status:MAVEN_SDC_COMPONENT for this job
            config_logging: The logging configuration name or none for env logging setup
            singleton: Boolean indicating if this job should be run as a singleton
            flavor_id: The singleton flavor_id
        '''
        self.std_capture = StringIO()
        sys.stdout = self.std_capture

    def run(self,
            proc,
            proc_args=None,
            **kwargs):
        ''' The main entry point for a cron status job.  This method will call the provided
            proc as the job entry point.
        Arguments:
            proc: The function that is the entry point of the job
            proc_args: A dictionary of arguments to be provided to proc
            propagate_exceptions: If true, exceptions are propagated
        '''
        super(StatusCronJob, self).run(proc, proc_args, ** kwargs)
        proc_name = proc.__name__

        std_out = self.std_capture.getvalue()

        if std_out and len(std_out) > 0:
            status_api.add_status(event_id=maven_status.MAVEN_SDC_EVENTS.OUTPUT,
                                  component_id=self.component_id,
                                  summary='{0} Stdout'.format(proc_name),
                                  description=std_out if len(std_out) < 2 * self.STD_OUT_LEN else std_out[:self.STD_OUT_LEN] + '\n...\n' + std_out[-self.STD_OUT_LEN:])
