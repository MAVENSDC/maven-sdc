'''
Created on May 19, 2015

@author: bstaley

'''
from multiprocessing import Queue


class DummyMessage():
    '''Dummy message class used in testing the sending of emails.'''

    def __init__(self, from_address, to_addresses, full_message):
        self.from_address = from_address
        self.to_addresses = to_addresses
        self.full_message = full_message


class DummySMTP():
    '''Dummy SMTP class used in testing the sending of emails.

    During testing, replace the real SMTP class by making this assignment.
    import smtplib
    smtplib.SMTP = self.DummySMTP
    '''
    messages = []

    def __init__(self, address, port):
        self.address = address

    def login(self, username, password):
        self.username = username
        self.password = password

    def sendmail(self, from_address, to_addresses, full_message):
        self.messages.append(DummyMessage(from_address=from_address,
                                          to_addresses=to_addresses,
                                          full_message=full_message))

    def quit(self):
        self.has_quit = True


class DummyMultiProcessSMTP():
    '''Dummy SMTP class used in testing the sending of emails.

    During testing, replace the real SMTP class by making this assignment.
    import smtplib
    smtplib.SMTP = self.DummySMTP
    '''
    messages = Queue()

    def __init__(self, address, port):
        self.address = address

    def login(self, username, password):
        self.username = username
        self.password = password

    def sendmail(self, from_address, to_addresses, full_message):
        self.messages.put(DummyMessage(from_address=from_address,
                                       to_addresses=to_addresses,
                                       full_message=full_message))

    def quit(self):
        self.has_quit = True


def drain(message_queue):
    results = []
    while message_queue.qsize():
        results.append(message_queue.get(block=False))
    return results
