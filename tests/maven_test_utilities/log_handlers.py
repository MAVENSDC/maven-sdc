'''
Created on Jun 11, 2015

@author: bstaley
'''
import logging


class RecordHandler(logging.Handler):
    '''Logging handler that records all logs that pass the logging criticality check'''
    records = []

    def emit(self, record):
        self.records.append(record)

    def clear(self):
        del self.records[:]

    def contains(self, string_to_check):
        for record in self.records:
            if string_to_check in record.msg:
                return True
        return False


class MultiProcessHandler(logging.Handler):
    '''Logging handler that provides logs between disparate processes'''
    from multiprocessing import Queue
    records = Queue()

    def emit(self, record):
        self.records.put(record)

    def clear(self):
        for _ in range(self.records.qsize()):
            self.records.get(block=False)

    def drain(self):
        results = []
        while self.records.qsize():
            results.append(self.records.get(block=False))
        return results


def get_records(in_records,
                level=None,
                msg_key_word=None):
    '''Method used to check the provided record list for level and/or keyword'''
    results = in_records
    if level:
        results = [x for x in results if x.levelno == level]
    if msg_key_word:
        msg_key_word = [x for x in results if msg_key_word in x.msg]
    return results
