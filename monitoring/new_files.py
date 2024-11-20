#! /usr/bin/env python
#
# A script to check for new MAVEN files in any directory
# or set of directories, and create a chunk of HTML giving
# links to the filenames and the creation time of the files,
# which can then be loaded in the website.
#
# Subdirs: whether to go through all subdirs or not.
#
# Example: python new_files.py '/maven/data/anc/spice/' True newfoo.html
# Example: python new_files.py '/maven/data/foo,maven/data/foo2' False newfoo.html
#
# Alex DeWolfe 2013-05-10
#
# Solution for listing files by time from Stack Overflow:
# http://stackoverflow.com/questions/6235639/how-to-make-a-chronological-list-of-files-with-the-file-modification-date
import os
from datetime import datetime
from maven_database.models import ScienceFilesMetadata, AncillaryFilesMetadata

display_time_format = '%Y-%m-%d %H:%M:%S'


def list_files(src_dir, subdirs, num_files):
    query = ScienceFilesMetadata.query
    anc_query = AncillaryFilesMetadata.query
    src_dir = src_dir[:-1] if src_dir[-1] == '/' else src_dir

    if subdirs:
        query = query.filter(ScienceFilesMetadata.directory_path.like(src_dir + '%'))
        anc_query = anc_query.filter(AncillaryFilesMetadata.directory_path.like(src_dir + '%'))
    else:
        query = query.filter(ScienceFilesMetadata.directory_path == src_dir)
        anc_query = anc_query.filter(AncillaryFilesMetadata.directory_path == src_dir)

    query = query.order_by(ScienceFilesMetadata.mod_date.desc()).limit(num_files)
    anc_query = anc_query.order_by(AncillaryFilesMetadata.mod_date.desc()).limit(num_files)

    # return [(os.path.join(next_sfm.directory_path, next_sfm.file_name), str(next_sfm.updated_last)) for next_sfm in query.all()]
    results = []
    for next_sfm in query.all():
        fn = os.path.join(next_sfm.directory_path, next_sfm.file_name)
        mod_date_str = next_sfm.mod_date.strftime(display_time_format)
        results.append((fn, mod_date_str))
    for next_sfm in anc_query.all():
        fn = os.path.join(next_sfm.directory_path, next_sfm.file_name)
        mod_date_str = next_sfm.mod_date.strftime(display_time_format)
        results.append((fn, mod_date_str))

    return results


def write_html(newfilenames, output):
    '''Writes list as HTML for use by the SDC website

    Arguments
        filenames - a list of filenames
    '''
    now = datetime.utcnow().strftime(display_time_format)

    '''Make new list with filename-TAB-filetime.'''
    filelist = [('<a href="{0}">{0}</a>&nbsp;{1}<br />').format(fn, time)
                for fn, time in newfilenames]

    '''Make list into one long string with line breaks'''
    liststring = '\n'.join(filelist)

    '''Add in some html'''
    htmlstring = ('<div id="newfiles" class="monospace">\n'
                  'Last checked {0}<br />\n{1}\n</div>').format(now, liststring)

    output.write(htmlstring)


def main(paths, subdirs, dst_file, num_files):
    '''Main:

    Arguments
        paths - list of dirs, e.g. '/maven/data/foo,maven/data/foo2'
        subdirs - a boolean whether to do subdirs in a single directory
        dest_file - e.g. /maven/data/sdc/new_anc_files/anc.html
        num_files - int, maximum number of files displayed, <= 0 displays all
    '''
    file_list = []
    paths = paths.split(',')
    for path in paths:
        file_list.extend(list_files(path, subdirs, num_files))

    with open(dst_file, 'w+') as html_output:
        write_html(file_list, html_output)
