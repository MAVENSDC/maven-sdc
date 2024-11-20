'''
Created on Mar 23, 2015

@author: tbussell

Tests of the mail module.
'''
import unittest
import mock
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
from maven_utilities import mail


class MailTestCase(unittest.TestCase):

    def sendmail_side_effect(self, *args):
        self.assertEqual('Sender Username', args[0])
        self.assertEqual(1, len(args[1]))
        self.assertEqual('mavensdc@lasp.colorado.edu', args[1][0])
        self.assertIn('Subject', args[2])
        self.assertIn('Message', args[2])
        self.assertIn('Sender Title', args[2])
        self.assertIn('Sender@email.com', args[2])
        self.assertIn('mavensdc@lasp.colorado.edu', args[2])

    def test_send_email(self):
        with mock.patch.object(target=smtplib.SMTP, attribute='sendmail', side_effect=self.sendmail_side_effect):
            mail.send_email(subject='Subject', message='Message', sender_username='Sender Username',
                            sender_title='Sender Title', sender_email='Sender@email.com', recipients=None)

    def sendmail_mime_side_effect(self, *args):
        self.assertEqual('Sender Username', args[0], "Is the email server running? The command to run the "
                                                     "server is: python -m smtpd -c DebuggingServer -n 127.0.0.1:1025")
        self.assertEqual(1, len(args[1]))
        self.assertEqual('mavensdc@lasp.colorado.edu', args[1][0])
        msg = args[2]
        self.assertIn('the subject', msg)
        self.assertIn('the message', msg)
        self.assertIn('Sender@email.com', msg)
        self.assertIn('mavensdc@lasp.colorado.edu', msg)
        self.assertIn('test_mail.py', msg)
        self.assertIn('Content-Transfer-Encoding: base64', msg)

    def test_send_mail_with_attachment(self):
        file_dir = os.path.dirname(os.path.abspath(__file__))
        attachment = os.path.join(file_dir, 'test_mail.py')
        with mock.patch.object(target=smtplib.SMTP, attribute='sendmail', side_effect=self.sendmail_mime_side_effect):
            mail.send_mime_email(subject='the subject', message='the message', sender_username='Sender Username',
                                 sender_title='Sender Title', sender_email='Sender@email.com', recipients=None,
                                 attachments=[attachment])

    def sendmail_mime_html_side_effect(self, *args):
        self.assertEqual('Sender Username', args[0])
        self.assertEqual(1, len(args[1]))
        self.assertEqual('mavensdc@lasp.colorado.edu', args[1][0])
        msg = args[2]
        self.assertIn('the subject', msg)
        self.assertIn('How are you', msg)
        self.assertIn('Sender@email.com', msg)
        self.assertIn('mavensdc@lasp.colorado.edu', msg)
        self.assertIn('<p>Hi!<br>', msg)

    def test_send_mail_html(self):
        message_html = '''<html>
  <head></head>
  <body>
    <p>Hi!<br>
       How are you?<br>
       Here is the <a href="https://www.python.org">link</a> you wanted.
    </p>
  </body>
</html>'''
        with mock.patch.object(target=smtplib.SMTP, attribute='sendmail', side_effect=self.sendmail_mime_html_side_effect):
            mail.send_mime_email(subject='the subject', message='the message', sender_username='Sender Username',
                                 sender_title='Sender Title', sender_email='Sender@email.com', recipients=None,
                                 message_html=message_html)

    def exception_sendmail_side_effect(self, *args):
        self.assertEqual('Sender Username', args[0])
        self.assertEqual(1, len(args[1]))
        self.assertEqual('mavensdc@lasp.colorado.edu', args[1][0])
        self.assertIn('Subject', args[2])
        self.assertIn('Message', args[2])
        self.assertIn('Test exception', args[2])
        self.assertIn('Sender Title', args[2])
        self.assertIn('Sender@email.com', args[2])
        self.assertIn('mavensdc@lasp.colorado.edu', args[2])

    def test_send_exception_email(self):
        try:
            raise Exception('Test exception')
        except Exception:
            with mock.patch.object(target=smtplib.SMTP, attribute='sendmail', side_effect=self.exception_sendmail_side_effect):
                mail.send_exception_email(subject='Subject', message='Message', sender_username='Sender Username',
                                          sender_title='Sender Title', sender_email='Sender@email.com', recipients=None)
