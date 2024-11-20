'''
Created on Oct 2, 2014

@author: Bryan Staley
'''

import os
from os.path import basename
import smtplib
import traceback
from maven_utilities import config, constants


def send_email(subject,
               message,
               sender_username=config.email_sender_username,
               sender_title=config.email_sender_title,
               sender_email=config.email_sender_email,
               recipients=None):
    '''Sends an email.
    Arguments
        subject - The email subject
        message - contents of the email
        sender_username - The system user name of the sender
        sender_title - The pretty print sender's title
        sender_email - The sender's email address
        recipients - The list of email address for the intended recipients
    '''

    if recipients is None:
        recipients = config.email_recipients

    send_from = 'From: %s <%s>' % (sender_title, sender_email)
    message_strings = [send_from]
    to_string = '<' + '>, <'.join(recipients) + '>'
    message_strings.append('To: %s' %
                           to_string)
    message_strings.append('Subject: %s' % subject)
    msg = '\n'.join(message_strings) + '\n\n' + message

    if os.environ[constants.python_env] == 'testing':
        email_port = 1025
    else:
        email_port = 25
    server = smtplib.SMTP('localhost', email_port)
    server.sendmail(sender_username, recipients, msg)


def send_mime_email(subject,
                    message,
                    sender_username=config.email_sender_username,
                    sender_title=config.email_sender_title,
                    sender_email=config.email_sender_email,
                    recipients=None,
                    attachments=None,
                    message_html=None):
    '''Sends an email using MIME multi-part format, allowing for attachments and html body.
    Arguments
        subject - The email subject
        message - contents of the email
        sender_username - The system user name of the sender
        sender_title - The pretty print sender's title
        sender_email - The sender's email address
        recipients - The list of email address for the intended recipients
        attachments - The list of files to attach to the email
        message_html - optional html-format message as a string

    Attachment code based on:
    http://stackoverflow.com/questions/3362600/how-to-send-email-attachments-with-python
    '''

    if recipients is None:
        recipients = config.email_recipients
        
    if "Kp Ingester" in subject:
        recipients.append('Brian.Mcclellan@lasp.colorado.edu')
    
    # this isn't used much, do the MIME imports only if required
    from email.mime.application import MIMEApplication
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    send_from = 'From: %s <%s>' % (sender_title, sender_email)
    send_to = ', '.join(recipients)

    # build the MIME multi-part msg
    msg = MIMEMultipart(
        From=send_from,
        To=send_to
    )
    msg['Subject'] = subject
    msg['To'] = send_to

    # we'd like to attach both plain and html
    # but some mail clients (Outlook Web App) act funny
    # so we use html first, or plain if there is no html
    if message_html:
        msg.attach(MIMEText(message_html, 'html'))
    elif message:
        msg.attach(MIMEText(message, 'plain'))

    # add any attachments
    for fn in attachments or []:
        with open(fn, "rb") as f:
            bn = basename(fn)
            msg.attach(MIMEApplication(
                f.read(),
                Content_Disposition='attachment; filename="%s"' % bn,
                Name=bn
            ))

    msg = msg.as_string()

    if os.environ[constants.python_env] == 'testing':
        email_port = 1025
    else:
        email_port = 25
    server = smtplib.SMTP('localhost', email_port)
    server.sendmail(sender_username, recipients, msg)


def send_exception_email(subject,
                         message,
                         sender_username=config.email_sender_username,
                         sender_title=config.email_sender_title,
                         sender_email=config.email_sender_email,
                         recipients=None):
    '''Sends an email.
    Arguments
        subject - The email subject
        message - contents of the email
        sender_username - The system user name of the sender
        sender_title - The pretty print sender's title
        sender_email - The sender's email address
        recipients - The list of email address for the intended recipients
    '''

    send_email(subject,
               message + '\n' + traceback.format_exc(),
               sender_username,
               sender_title,
               sender_email,
               recipients)
