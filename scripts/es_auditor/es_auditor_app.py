#!/home/brst8588/audit_py2/bin/python
'''
Created on Apr 20, 2016

@author: bstaley
'''
import os
import time
import argparse
from elasticsearch.client import Elasticsearch
from collections import OrderedDict
import logging

from maven_data_file_indexer import audit_utilities
from maven_status import job
import maven_status

logger = logging.getLogger('maven.maven_tools.es_auditor')


class fileModRecord(OrderedDict):

    def __init__(self,
                 file_name,
                 change_type,
                 file_size,
                 when
                 ):
        '''Record constructor
        Arguments:
            file_name : The record file name
            change_type : The change type detected (missing from disk, missing from db, differs)
            file_size : The size of the file in bytes
        '''
        OrderedDict.__init__(self)
        self['file_name'] = file_name
        self['change_type'] = change_type
        self['file_size'] = file_size
        self['when'] = when

    def asJson(self):
        '''Method used to serialize this in json'''
        import json
        return json.dumps(self)

    def asKVP(self):
        return 'file_name:{0} change_type:{1} file_size:{2} when:{3}'.format(self['file_name'],
                                                                             self['change_type'],
                                                                             self['file_size'],
                                                                             self['when'])


def parse_args():
    '''Helper method used to process command line arguments'''
    parser = argparse.ArgumentParser(description='''''')
    parser.add_argument('-H', '--host',
                        default='localhost')
    parser.add_argument('-p', '--port',
                        type=int,
                        default=2222)
    parser.add_argument('-d', '--directories',
                        nargs='*',
                        default=['/maven/data/sci/', '/maven/data/anc/'])
    parser.add_argument('-o', '--output',
                        choices=['elastic', 'udp', 'text'],
                        default='upd')

    args = parser.parse_args()
    return args


def getSafeInt(val,
               def_val=0):
    '''Helper method used to safe cast an int.
    Arguments:
        val : Value to be cast
        def_val : The default value to be provided if val can't be cast
    Returns:
        An integer
    '''
    try:
        return int(val)
    except:
        logger.info('{0} can not be cast as an integer, using default {1}'.format(val, def_val))
        return def_val


def runAudit(directories,
             host,
             port,
             output_type):

    def recordElastic(record, host, port, index, doc_type):
        '''Method used to add the provided record to elastic search
        Arguments:
            record : Record to be added
            host : ES host
            port : ES port
            index : ES index
            doc_type : ES type
        '''
        es_transport = Elasticsearch(hosts=[{'host': host,
                                             'port': port}])
        es_transport.indices.create(index=index, ignore=400)
        es_transport.index(index=index,
                           doc_type=doc_type,
                           body=record.asJson())

    def elasticOut(record, host, port, **kwargs):
        recordElastic(record,
                      host=host,
                      port=port,
                      index='maven',
                      doc_type='audit_modifications')

    def udpOut(record, host, port, **kwargs):
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        sock.sendto(record.asKVP(), (host, port))

    def textOut(record, **kwargs):
        print record.asKVP()

    output = None

    if output_type == "text":
        output = textOut
    elif output_type == "udp":
        output = udpOut
    elif output_type == "elastic":
        output = elasticOut

    sample_time = int(time.time() * 1000)

    add_cnt, del_cnt, upd_cnt = 0, 0, 0

    for directory in directories:
        logger.info('Processing:\n\tDir: %s\n\tHost: %s\n\tPort: %s', directory, host, port)

        disk_meta = audit_utilities.get_metadata_from_disk(directory)
        db_meta = audit_utilities.get_metadata_from_db(directory)
        add, delete, update = audit_utilities.get_metadata_diffs(db_meta, disk_meta)

        add_cnt += len(add)
        del_cnt += len(delete)
        upd_cnt += len(update)

        for _next in update:
            try:
                logger.info("changed_results = {0}".format(_next))
                change_record = fileModRecord(file_name=os.path.basename(_next.path_name),
                                              change_type='CHANGED',
                                              file_size=getSafeInt(_next.file_size),
                                              when=sample_time)
                output(change_record, host=host, port=port)
            except IndexError:
                logger.error("index error occured on sc in changed_results for '{0}'".format(_next))
                raise RuntimeError('unable to index changed_results, logged exception and processing stopped')

        for _next in delete:
            change_record = fileModRecord(file_name=os.path.basename(_next.path_name),
                                          change_type='MISSING_FROM_DISK',
                                          file_size=getSafeInt(_next.file_size),
                                          when=sample_time)

            output(change_record, host=host, port=port)

        for _next in add:
            change_record = fileModRecord(file_name=os.path.basename(_next.path_name),
                                          change_type='MISSING_FROM_DB',
                                          file_size=getSafeInt(_next.file_size),
                                          when=sample_time)

            output(change_record, host=host, port=port)

    maven_status.status.add_status(component_id=maven_status.MAVEN_SDC_COMPONENT.ES_AUDITOR,
                                   event_id=maven_status.MAVEN_SDC_EVENTS.STATUS,
                                   summary='ES Audit Finished with:\n\t {0} Changed\n\t {1} Disk Missing\n\t {2} DB Missing'.format(upd_cnt,
                                                                                                                                    add_cnt,
                                                                                                                                    del_cnt))


if __name__ == '__main__':
    args = parse_args()

    with job.StatusJob(component_id=maven_status.MAVEN_SDC_COMPONENT.ES_AUDITOR) as job:
        job.run(proc=runAudit,
                proc_args={"directories": args.directories,
                           "host": args.host,
                           "port": args.port,
                           "output_type": args.output})
