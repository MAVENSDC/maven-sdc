# pylint: disable=E1101
'''
Created on Mar 14, 2016

@author: bstaley
'''

import os
import logging
import pyinotify
from multiprocessing import Queue, Process
from . import index_worker
from maven_utilities import time_utilities
from maven_status import MAVEN_SDC_COMPONENT, MAVEN_SDC_EVENTS, status

logger = logging.getLogger('maven.maven_data_file_indexer.maven_delta_indexer.log')

work_queue = Queue()
work_queue.cancel_join_thread()  # ensures we can terminate with items left on the work queue
error_queue = Queue()
error_queue.cancel_join_thread()  # ensures we can terminate with items left on the work queue
number_of_workers = 1


class DeltaIndexer():
    '''Class used to perform delta index updates (index updates that assume the index cache is
    correct as there is no complete index reconciliation with this class).  In the event this indexer
    is interrupted (either by process crash or Q overflow) a full index needs to be done prior to
    starting up the DeltaIndexer'''

    class INotifyHandler(pyinotify.ProcessEvent):

        def my_init(self, **kargs):
            '''
            Constructor
            Arguments:
                delete_handlers - The list of handlers to be called when a file is determined to be deleted
                close_handlers - The list of handlers to be called when a file is determined to be created or modified
                q_overflow_handlers - The list of handlers to be called when a q overflow is detected
            '''
            pyinotify.ProcessEvent.my_init(self, **kargs)

            self.delete_handlers = kargs.get('delete_handlers', [])
            self.close_handlers = kargs.get('close_handlers', [])
            self.q_overflow_handlers = kargs.get('q_overflow_handlers', [])

            logger.info('%s created ' % self)

        def process_IN_Q_OVERFLOW(self, event):
            '''Q Overflow handler'''
            pyinotify.ProcessEvent.process_IN_Q_OVERFLOW(self, event)
            for h in self.q_overflow_handlers:
                h(event)

        def process_IN_CLOSE_WRITE(self, event):
            '''File modify/create handler'''
            logger.debug('Handling close_write event for %s', event.pathname)
            for h in self.close_handlers:
                h(event)

        def process_IN_DELETE(self, event):
            '''File delete handler'''
            logger.debug('Handling delete event for %s', event.pathname)
            for h in self.delete_handlers:
                h(event)

        def __str__(self):
            return '%s - \n\tdel_handlers %s\n\t, close_handlers %s\n\t, q_overflow_handlers %s' % (self.__class__.__name__,
                                                                                                    self.delete_handlers,
                                                                                                    self.close_handlers,
                                                                                                    self.q_overflow_handlers)

    def __init__(self,
                 base_directories,
                 delete_handlers=None,
                 close_handlers=None,
                 q_overflow_handlers=None
                 ):
        '''Constructor.  Processing doesn't start until process_events(...) is called.
        Arguments:
            base_directories - The base directories to watch.  All child directories will also be watched.
            delete_handlers - The list of handlers to be called when a file is determined to be deleted
            close_handlers - The list of handlers to be called when a file is determined to be created or modified
            q_overflow_handlers - The list of handlers to be called when a q overflow is detected'''

        for base_directory in base_directories:
            assert os.path.isdir(base_directory), "%s doesn't exist!" % base_directory

        self.notif_handler = self.INotifyHandler(delete_handlers=delete_handlers,
                                                 close_handlers=close_handlers,
                                                 q_overflow_handlers=q_overflow_handlers)

        self.base_directories = base_directories
        logger.info('%s created ' % self)

    def process_events(self):
        '''Event processing loop.  This method doesn't return until an error occurs or a signal
        (e.g ctrl-c) is sent to the process and handles file events asynchronously.'''

        logger.info('%s starting processing of events on %s' % (self, self.base_directories))
        wm = pyinotify.WatchManager()  # Watch Manager
        mask = pyinotify.IN_DELETE | pyinotify.IN_CLOSE_WRITE | pyinotify.IN_Q_OVERFLOW

        try:
            notifier = pyinotify.Notifier(wm, self.notif_handler)
            for base_directory in self.base_directories:
                _ = wm.add_watch(base_directory, mask, rec=True, auto_add=True)
                logger.info('Delta indexer watching %s', base_directory)

            worker_processes = []

            for _ in range(number_of_workers):
                logger.info('Starting another worker process')
                next_process = Process(target=index_worker.process_file_events,
                                       args=(work_queue, error_queue))
                next_process.start()
                worker_processes.append(next_process)
                logger.info('Worker process started')

            logger.info('%s starting processing loop!', self)
            notifier.loop()
            logger.error('%s stopped processing loop!', self)
        except KeyboardInterrupt as _:
            # Q overflow happened
            pass
        except Exception as _:
            logger.exception('INotify event processing encountered an error')
            status.add_exception_status(component_id=MAVEN_SDC_COMPONENT.DELTA_INDEXER,
                                        event_id=MAVEN_SDC_EVENTS.FAIL,
                                        summary='Delta Indexer terminated unexpectedly for %s' % self)
        else:
            status.add_status(component_id=MAVEN_SDC_COMPONENT.DELTA_INDEXER,
                              event_id=MAVEN_SDC_EVENTS.STATUS,
                              summary='Delta Indexer terminated',
                              description='Delta Indexer terminated %s' % self)

        finally:
            for next_process in worker_processes:
                logger.info('Terminating %s', next_process)
                next_process.terminate()
            for next_process in worker_processes:
                logger.info('Joining %s', next_process)
                next_process.join()

    def __str__(self):
        return '%s -  base_directory %s' % (self.__class__.__name__,
                                            self.base_directories)


def indexer_close_handler(event):
    '''Indexer handler for file close (includes create and modify file actions).
    Arguments:
        event - The file event from inotify
    '''
    work_queue.put(index_worker.FileEvent(event.pathname,
                                          index_worker.FILE_EVENT.CLOSED,
                                          time_utilities.utc_now()
                                          ))


def indexer_rem_handler(event):
    '''Indexer handler for file removals.
    Arguments:
        event - The file event from inotify
    '''
    work_queue.put(index_worker.FileEvent(event.pathname,
                                          index_worker.FILE_EVENT.REMOVED,
                                          time_utilities.utc_now()
                                          ))

# pylint: disable=W0613


def check_worker(event):
    '''Handler used to determine if any workers have reported an error
    Arguments:
        event - Not used
    '''
    if error_queue.qsize() > 0:
        worker_exception = error_queue.get()
        logger.exception('Index worker quit with %s!', worker_exception)
        raise RuntimeError(worker_exception)


def indexer_q_overflow_handler(event):
    '''Q Overflow handler.
    Arguments:
        event - Reported
    '''
    import socket
    hostname = socket.gethostname()
    err_msg = 'INotify Queue Overflow detected on host %s, event %s' % (hostname, event)
    logger.error(err_msg)
    status.add_status(component_id=MAVEN_SDC_COMPONENT.DELTA_INDEXER,
                      event_id=MAVEN_SDC_EVENTS.STATUS,
                      summary=err_msg)

    logger.warning('Stopping the event processing!')
    # Stop the event processing
    raise KeyboardInterrupt


def get_root_indexer(root_directories):
    '''Method used to return a default DeltaIndexer'''
    return DeltaIndexer(base_directories=root_directories,
                        delete_handlers=[indexer_rem_handler, check_worker],
                        close_handlers=[indexer_close_handler, check_worker],
                        q_overflow_handlers=[indexer_q_overflow_handler, check_worker])
