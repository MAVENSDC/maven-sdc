#!/usr/bin/env python
'''
Created on Aug 11, 2016

@author: bstaley
'''
from datetime import datetime
import operator
from jinja2 import Environment, PackageLoader
import os
import re
from sqlalchemy import or_
import argparse
import sys
from dateutil import parser
import pytz

from maven_utilities import time_utilities, mail, singleton
from maven_database import db_session
from maven_database.models import MavenStatus
import maven_status

env = Environment(loader=PackageLoader('maven_status', 'templates'))


def get_last_run(last_ran_loc):
    '''Helper method used to retrieve the last time this application was successfully run
    Arguments:
        last_ran_loc: Fully qualified path to the file that houses the last ran time
    Returns:
        The last ran time
    '''
    run_time = None
    if not os.path.isfile(last_ran_loc):
        run_time = datetime.min.replace(tzinfo=pytz.UTC)
    else:
        with open(last_ran_loc, 'r+') as last_ran:
            last_ran_datetime = last_ran.read()
            try:
                run_time = parser.parse(last_ran_datetime).replace(tzinfo=pytz.UTC)
            except:
                raise Exception("[{}] wasn't a valid time format".format(last_ran_datetime))
    return run_time


def store_last_run(last_ran_loc):
    '''Helper method used to store the last successful ran time
    Arguments:
        last_ran_loc: Fully qualified path to the file that houses the last ran time
    '''
    with open(last_ran_loc, 'w+') as last_ran:
        last_ran.write(time_utilities.utc_now().isoformat())


def get_affected_components(statuses):
    '''Helper method used to rearrange the status information into {component_id:[status]}
    Arguments:
        statuses: The list of MavenStatus objects retrieved from the database
    Returns:
        (job_id:{component_id:{event_id:[status]}}}
    '''
    affected_components = {}
    for _next in statuses:
        affected_components.setdefault(_next.job_id, {}).setdefault(_next.component_id, {}).setdefault(_next.event_id, []).append(_next)
    return affected_components


def run_summary_status(components,
                       events,
                       template,
                       recipients,
                       from_dt,
                       to_dt,
                       max_num_status=None):
    '''Main method used to determine if there are any statuses of interest and 
    send a report email
    Arguments:
        components: A list of MAVEN_SDC_COMPONENT in string representation
        events: A list of MAVEN_SDC_EVENTS in string representation
        template: A template filename used to format the email message body
        recipients: A list of recipient email addresses
        from_dt: The status query start time
        to_dt: The status query end time
    '''
    max_num_status = max_num_status or 10
    
    # query for recent statuses
    query = db_session.query(MavenStatus)

    query = query.filter(MavenStatus.timetag >= from_dt). \
        filter(MavenStatus.timetag < to_dt)

    component_filter = []
    for component in components:
        component_filter.append(operator.eq(MavenStatus.component_id, component))
    query = query.filter(or_(*component_filter))

    event_filter = []
    for event in events:
        event_filter.append(operator.eq(MavenStatus.event_id, event))
    query = query.filter(or_(*event_filter))

    # group and order
    query = query.order_by(MavenStatus.job_id.desc(), MavenStatus.timetag.desc()).limit(max_num_status)

    statuses = query.all()
    if statuses is None or len(statuses) == 0:
        return

    ''' 
    affected_components => {job_id:{component_id:{event_id:[status]}}}
    from_td => datetime
    to_dt => datetime
    '''
    job_statuses = get_affected_components(statuses)

    def pretty_print(caps_string):
        return re.sub(r'([A-Z]{1})([A-Z]+)',
                      lambda m: m.group(1) + m.group(2).lower(),
                      re.sub(r'_', r' ', caps_string))

    for _next_job in job_statuses:
        # Pretty Print Info
        component_statuses = job_statuses[_next_job]
        for _next in list(component_statuses.keys()): 
            evt_keys = list(component_statuses[_next].keys())
            for _next_event in evt_keys:
                component_statuses[_next][pretty_print(_next_event)] = component_statuses[_next].pop(_next_event)
            component_statuses[pretty_print(_next)] = component_statuses.pop(_next)

        subject = ' '.join(component_statuses.keys()) + ' - ' + ' '.join(pretty_print(e) for e in events)
        # format data
        template = env.get_template(template)

        html_body = template.render(affected_components=job_statuses[_next_job],
                                    job_id=_next_job,
                                    from_dt=from_dt.replace(tzinfo=None, second=0, microsecond=0),
                                    to_dt=to_dt.replace(tzinfo=None, second=0, microsecond=0))
        
        mail.send_mime_email(subject=subject,
                             message=None,
                             recipients=recipients,
                             message_html=html_body)


def parse_arguments(arguments):
    '''Parses arguments from main()'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--components',
                        help='''Components to status''',
                        nargs='+',
                        choices=[i.name for i in maven_status.MAVEN_SDC_COMPONENT])
    parser.add_argument('-e', '--events',
                        help='''Events to status''',
                        nargs='+',
                        choices=[i.name for i in maven_status.MAVEN_SDC_EVENTS])
    parser.add_argument('-t', '--template',
                        help='''Email template to use''')
    parser.add_argument('-r', '--recipients',
                        help='''Email recipient list''',
                        nargs='+')
    parser.add_argument('-f', '--flavor-id',
                        help='The unique name of this daemon instance',
                        default='email_status_app')
    parser.add_argument('-m', '--max-emails',
                        help='Send at most this many recent statuses',
                        default=10)
    return parser.parse_args(arguments)


if __name__ == '__main__':
    
    # Run as a singleton
    _ = singleton.SingleInstance()
    args = parse_arguments(sys.argv[1:])
    last_ran_loc = os.path.join('/tmp', '.{0}_last_run'.format(args.flavor_id))
    from_dt = get_last_run(last_ran_loc)
    to_dt = time_utilities.utc_now()

    run_summary_status(components=args.components,
                       events=args.events,
                       template=args.template,
                       recipients=args.recipients,
                       from_dt=from_dt,
                       to_dt=to_dt,
                       max_num_status=args.max_emails)
    store_last_run(last_ran_loc)
