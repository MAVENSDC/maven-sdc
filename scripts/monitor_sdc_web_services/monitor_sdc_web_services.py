#! /usr/bin/env python
'''
Created on Sep 24, 2015

@author: cosc3564
'''
'''
    A script to hit the MAVEN web services in the SDC to see if they are functioning
'''
import os
import json
import sys
import random
import requests
import logging
from maven_status import job, status, MAVEN_SDC_COMPONENT, MAVEN_SDC_EVENTS
import argparse

# MAVEN web services

'''
For all files, the test hits the MAVEN API to check if it works

If it does not work, an email is sent to those in the list of recipients
'''

logger = logging.getLogger("maven.monitor_sdc_web_services.log")
logger_console = logging.getLogger("maven.monitor_sdc_web_services.console")
server = None
url_type = None
test_failures = []


def capture_failures(summary, details=None):
    test_failures.append((summary, details))


def try_maven_science_files():
    '''Hits the MAVEN API for the science files to check if they work '''
    url = 'http://%s/maven/sdc/%s/files/api/v1/search/science/fn_metadata/file_info?start_date=2015-08-03&end_date=2015-08-03' % (
        server, url_type)
    logger.info("maven_science_files URL %s" % url)
    r = requests.get(url)
    if not r.ok:
        capture_failures('MAVEN %s web service that lists science file info failed' % server,
                         '%d: %s\n%s' % (r.status_code, r.url, r.text))
        return False
    else:
        d = r.json()
        if 'files' in d and len(d['files']) > 0:
            f = random.choice(d['files'])
            bn = f['file_name']
            url = 'http://%s/maven/sdc/%s/files/api/v1/search/science/fn_metadata/download?file=%s' % (
                server, url_type, bn)
            logger.info("maven_science_files GET files URL %s" % url)
            r = requests.get(url)
            if not r.ok:
                capture_failures('MAVEN %s web service that downloads science file failed' % server,
                                 '%d: %s\n%s' % (r.status_code, r.url, r.text))
                return False
    return True


def try_maven_kp_values_file_names():
    '''Hits the MAVEN API for the kp file name value to check it they work'''
    url = 'http://%s/maven/sdc/%s/files/api/v1/search/science/kp_values/file_names?timestamp>=2015-05-16T02:00:00.0001Z&timestamp<2015-05-16T02:15:00.0001Z' % (
        server, url_type)
    logger.info("maven_kp_values_file_names URL %s" % url)
    r = requests.get(url)
    if not r.ok:
        capture_failures('MAVEN %s web services that lists kp filenames failed' % server,
                         '%d: %s\n%s' % (r.status_code, r.url, r.text))
        return False
    else:
        fns = r.text.split(',')
        if len(fns) > 0:
            url = 'http://%s/maven/sdc/%s/files/api/v1/search/science/kp_values/download?file=%s&timestamp>=2015-05-16T01:00:00.0001Z&timestamp<2015-05-16T01:15:00.0001Z' % (
                server, url_type, fns[0])
            logger.info("maven_kp_values_file_names GET files URL %s" % url)
            r = requests.get(url)
            if not r.ok:
                capture_failures('MAVEN %s web service that downloads kp value filenames failed' % server,
                                 '%d: %s\n%s' % (r.status_code, r.url, r.text))
                return False
    return True


def try_maven_kp_values_file_info():
    '''Hits the web service that provides kp file info'''
    url = 'http://%s/maven/sdc/%s/files/api/v1/search/science/kp_values/file_info?timestamp>=2014-05-16T01:00:00.0001Z&timestamp<2014-05-23T01:00:00.0001Z' % (
        server, url_type)
    logger.info("maven_kp_values_file_info URL %s" % url)
    r = requests.get(url)
    if not r.ok:
        capture_failures('MAVEN %s web service that lists kp values file info has failed' % server,
                         '%d: %s\n%s' % (r.status_code, r.url, r.text))
        return False
    return True


def try_maven_fn_metadata_file_name():
    '''Hits the MAVEN API for the metadata filenames'''
    url = 'http://%s/maven/sdc/%s/files/api/v1/search/science/fn_metadata/file_names?start_date=2015-08-03&end_date=2015-08-03' % (
        server, url_type)
    logger.info("maven_fn_metadata_file_name URL %s" % url)
    r = requests.get(url)
    if not r.ok:
        capture_failures('MAVEN %s web service that lists metadata filenames failed' % server,
                         '%d: %s\n%s' % (r.status_code, r.url, r.text))
        return False
    else:
        fns = r.text.split(',')
        if len(fns) > 0:
            fn = random.choice(fns)
            bn = os.path.basename(fn)
            url = 'http://%s/maven/sdc/%s/files/api/v1/search/science/fn_metadata/download?file=%s' % (
                server, url_type, bn)
            logger.info("maven_fn_metadata_file_name GET file URL %s" % url)
            r = requests.get(url)
            if not r.ok:
                capture_failures('MAVEN %s web service that downloads metadata filenames failed' % server,
                                 '%d: %s\n%s' % (r.status_code, r.url, r.text))
                return False
    return True


def try_maven_fn_metadata_file_info():
    '''Hits the MAVEN API for the metadata file info'''
    url = 'http://%s/maven/sdc/%s/files/api/v1/search/science/fn_metadata/file_info?start_date=2015-08-03&end_date=2015-08-03' % (
        server, url_type)
    logger.info("maven_fn_metadata_file_info URL %s" % url)
    r = requests.get(url)
    if not r.ok:
        capture_failures('MAVEN %s web service that lists metadata file info failed' % server,
                         '%d: %s\n%s' % (r.status_code, r.url, r.text))
        return False

    else:
        json_results = json.loads(r.text)
        files = json_results['files']
        bn = random.choice(files)['file_name']
        url = 'http://%s/maven/sdc/%s/files/api/v1/search/science/fn_metadata/download?file=%s' % (
            server, url_type, bn)
        logger.info("maven_fn_metadata_file_info GET file URL %s" % url)
        r = requests.get(url)
        if not r.ok:
            capture_failures('MAVEN %s web service that downloads metadata file info failed' % server,
                             '%d: %s\n%s' % (r.status_code, r.url, r.text))
            return False
    return True


def try_maven_fn_metadata_file_info_zip():
    '''Hits the MAVEN API for the metadata file info'''
    url = 'http://%s/maven/sdc/%s/files/api/v1/search/science/fn_metadata/file_info?start_date=2015-08-03&end_date=2015-08-03' % (
        server, url_type)
    logger.info("maven_fn_metadata_file_info URL %s" % url)
    r = requests.get(url)
    if not r.ok:
        capture_failures('MAVEN %s web service that lists metadata file info failed' % server,
                         '%d: %s\n%s' % (r.status_code, r.url, r.text))
        return False

    else:
        json_results = json.loads(r.text)
        files = json_results['files']
        bn = random.choice(files)['file_name']
        url = 'http://%s/maven/sdc/%s/files/api/v1/search/science/fn_metadata/download_zip?file=%s' % (
            server, url_type, bn)
        logger.info("maven_fn_metadata_file_info GET file URL %s" % url)
        r = requests.get(url)
        if not r.ok:
            capture_failures('MAVEN %s web service that downloads zipped metadata file info failed' % server,
                             '%d: %s\n%s' % (r.status_code, r.url, r.text))
            return False
    return True


def try_maven_anc_metadata_file_name():
    '''Hits the MAVEN API for the metadata file name for ancillary routes'''
    url = 'http://%s/maven/sdc/%s/anc_files/api/v1/search/ancillary/file_names?start_date=2015-08-03&end_date=2015-08-03' % (
        server, url_type)
    logger.info("maven_anc_values_file_names URL %s" % url)
    r = requests.get(url)
    if not r.ok:
        capture_failures('MAVEN %s web services that lists ancillary filenames failed' % server,
                         '%d: %s\n%s' % (r.status_code, r.url, r.text))
        return False
    else:
        fns = r.text.split(',')
        if len(fns) > 0:
            fn = random.choice(fns)
            bn = os.path.basename(fn)
            url = 'http://%s/maven/sdc/%s/anc_files/api/v1/search/ancillary/fn_metadata/download?file=%s' % (
                server, url_type, bn)
            logger.info("maven_anc_values_file_names GET files URL %s" % url)
            r = requests.get(url)
            if not r.ok:
                capture_failures('MAVEN %s web service that downloads anc value files failed' % server,
                                 '%d: %s\n%s' % (r.status_code, r.url, r.text))
                return False
    return True

def try_maven_anc_metadata_download_zip():
    '''Hits the MAVEN API for download zip ancillary routes'''
    url = 'http://%s/maven/sdc/%s/anc_files/api/v1/search/ancillary/file_names?start_date=2015-08-03&end_date=2015-08-03' % (
        server, url_type)
    logger.info("maven_anc_values_file_names URL %s" % url)
    r = requests.get(url)
    if not r.ok:
        capture_failures('MAVEN %s web services that lists ancillary filenames failed' % server,
                         '%d: %s\n%s' % (r.status_code, r.url, r.text))
        return False
    else:
        fns = r.text.split(',')
        assert len(fns) > 0
        if len(fns) > 0:
            fn = random.choice(fns)
            bn = os.path.basename(fn)
            url = 'http://%s/maven/sdc/%s/anc_files/api/v1/search/ancillary/fn_metadata/download_zip?file=%s' % (
                server, url_type, bn)
            logger.info("maven_anc_values_file_names GET files URL %s" % url)
            r = requests.get(url)
            if not r.ok:
                capture_failures('MAVEN %s web service that downloads anc value files failed' % server,
                                 '%d: %s\n%s' % (r.status_code, r.url, r.text))
                return False
    return True


def try_maven_anc_metadata_file_info():
    '''Hits the MAVEN API for the metadata file info for ancillary routes'''
    url = 'http://%s/maven/sdc/%s/anc_files/api/v1/search/ancillary/file_info?start_date=2015-08-03&end_date=2015-08-03' % (
        server, url_type)
    logger.info("maven_anc_values_file_info URL %s" % url)
    r = requests.get(url)
    if not r.ok:
        capture_failures('MAVEN %s web service that lists ancillary values file info has failed' % server,
                         '%d: %s\n%s' % (r.status_code, r.url, r.text))
        return False
    else:
        json_results = json.loads(r.text)
        files = json_results['files']
        bn = random.choice(files)['file_name']
        url = 'http://%s/maven/sdc/%s/anc_files/api/v1/search/ancillary/fn_metadata/download?file=%s' % (
            server, url_type, bn)
        logger.info('maven_anc_values_file_info GET files URL %s' % url)
        r = requests.get(url)
        if not r.ok:
            capture_failures('MAVEN %s web service that downloads anc file failed' % server,
                             '%d: %s\n%s' % (r.status_code, r.url, r.text))
            return False
    return True


def try_maven_by_date_cache():
    '''Hits the MAVEN API returning counts and total sizes in cache'''
    url = 'http://%s/maven/sdc/%s/data_availability/api/v1/by_date' % (
        server, url_type)
    logger.info("maven_by_date_cache URL %s" % url)
    r = requests.get(url)
    if not r.ok:
        capture_failures('MAVEN %s web service that lists files by date [cached] failed' % server,
                         '%d: %s\n%s' % (r.status_code, r.url, r.text))
        return False
    return True


def try_maven_by_date_requery():
    '''Hits the MAVEN API returning counts and total sizes by requery'''
    url = 'http://%s/maven/sdc/%s/data_availability/api/v1/by_date?requery=1' % (
        server, url_type)
    logger.info("maven_by_date_requery URL %s" % url)
    r = requests.get(url)
    if not r.ok:
        capture_failures('MAVEN %s web service that lists files by date [requery] failed' % server,
                         '%d: %s\n%s' % (r.status_code, r.url, r.text))
        return False
    return True


def try_maven_events():
    '''Hits the MAVEN API for a list of events'''
    url = 'http://%s/maven/sdc/%s/events/api/v1/events?start_date=2015-08-03&end_date=2015-08-03' % (
        server, url_type)
    logger.info("maven_events URL %s" % url)
    r = requests.get(url)
    if not r.ok:
        capture_failures('MAVEN %s web service that list events has failed' % server,
                         '%d: %s\n%s' % (r.status_code, r.url, r.text))
        return False
    return True


def try_maven_all_events():
    '''Hits the MAVEN API for a list of all the events'''
    url = 'http://%s/maven/sdc/%s/events/api/v1/events/all.csv' % (
        server, url_type)
    logger.info("maven_all_events URL %s" % url)
    r = requests.get(url)
    if not r.ok:
        capture_failures('MAVEN %s web service that list all events has failed' % server,
                         '%d: %s\n%s' % (r.status_code, r.url, r.text))
        return False
    return True


def try_maven_selected_events():
    '''Hits the MAVEN API for a list of selected events'''
    url = 'http://%s/maven/sdc/%s/events/api/v1/events/selected.csv?start_date=2015-08-03&end_date=2015-08-03' % (
        server, url_type)
    logger.info("maven_selected_events URL %s" % url)
    r = requests.get(url)
    if not r.ok:
        capture_failures('MAVEN %s web service that list only selected events has failed' % server,
                         '%d: %s\n%s' % (r.status_code, r.url, r.text))
        return False
    return True


def try_maven_ranged_events():
    '''Hits the MAVEN API for a list of events in a select range'''
    url = 'http://%s/maven/sdc/%s/events/api/v1/events/ranged.csv?start_date=2014-05-16&end_date=2014-05-23' % (
        server, url_type)
    logger.info("maven_ranged_events URL %s" % url)
    r = requests.get(url)
    if not r.ok:
        capture_failures('MAVEN %s web service that lists a range of events has failed' % server,
                         '%d: %s\n%s' % (r.status_code, r.url, r.text))
        return False
    return True


def logger_stdout_report_error(error_msg, detail_msg):
    logger_console.info(error_msg, '\n', detail_msg)


def parse_arguments(arguments):
    '''Parses arguments from main()'''
    parser = argparse.ArgumentParser()
    parser.add_argument('server',
                        help='''server directory used to monitor the web services (i.e. "sdc-int1, sdc-web1")''')
    parser.add_argument('url',
                        help='''url_type pathway to server (team, public)''')
    parser.add_argument('-r', '--report-type',
                        help='''provides the report type for errors, an email by default''',
                        choices=['email', 'stdout'],
                        default='email')
    return parser.parse_args(arguments)


def main(args):
    global server, url_type

    server = args.server
    if server is None:
        print('server not defined, given sys.argv[1] as args.server: "%s"' % args.server)
        sys.exit(-1)

    url_type = args.url
    if url_type is None:
        print('url_type not defined, given sys.argv[2] as args.url: "%s"' % args.url)
        sys.exit(-1)

    critical_count = 0
    failure_count = 0
    success_count = 0
    test_methods = [try_maven_science_files,
                    try_maven_kp_values_file_names,
                    try_maven_kp_values_file_info,
                    try_maven_fn_metadata_file_name,
                    try_maven_fn_metadata_file_info,
                    try_maven_fn_metadata_file_info_zip,
                    try_maven_anc_metadata_file_name,
                    try_maven_anc_metadata_file_info,
                    try_maven_by_date_cache,
                    try_maven_by_date_requery,
                    try_maven_events,
                    # Takes a long time..  try_maven_all_events,
                    try_maven_selected_events,
                    try_maven_ranged_events]

    for test_method in test_methods:
        try:
            if test_method():
                success_count += 1
            else:
                failure_count += 1
        except Exception:
            critical_count += 1
            error_msg = 'monitor_sdc_web_services.py unable to access MAVEN files with server "%s" under "%s"' % (
                server, url_type)
            logger.exception(error_msg)
            detail_msg = 'monitor_sdc_web_service test method "%s" has failed' % test_method.__name__
            if args.report_type == 'stdout':
                logger_stdout_report_error(error_msg, detail_msg)
            status.add_exception_status(component_id=MAVEN_SDC_COMPONENT.WEB_MONITOR,
                                        event_id=MAVEN_SDC_EVENTS.FAIL,
                                        summary=detail_msg)
    if len(test_failures) > 0:
        error_msg = 'monitor_sdc_web_services.py includes test methods that fail with server "%s" under "%s"' % (
            server, url_type)
        detail_msg = 'monitor_sdc_web_services.py test method failures: \n\n\t%s' % '\n\t'.join(
            "%s-%s" % (summary, details) for summary, details in test_failures)

        if args.report_type == 'stdout':
            logger_stdout_report_error(error_msg, detail_msg)

        status.add_status(component_id=MAVEN_SDC_COMPONENT.WEB_MONITOR,
                          event_id=MAVEN_SDC_EVENTS.FAIL,
                          summary=error_msg,
                          description=detail_msg)

    logger.info(
        "%s web service tests with '%s' results:\n\t Total: %s\n\t Success: %s\n\t Failure: %s\n\t Critical Failure: %s",
        server,
        url_type,
        len(test_methods),
        success_count,
        failure_count,
        critical_count)


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])
    with job.StatusCronJob(MAVEN_SDC_COMPONENT.WEB_MONITOR, flavor_id="%s-%s" % (args.server, args.url)) as job:
        job.run(proc=main,
                proc_args={'args': args})