#! /usr/bin/env bash
#
# A bash script to run the script that generates an archive csv
# of all (SDC and OPs) events.
#
# Bryan Staley 2015-06-01

current_time=`date +"%Y-%m-%dT%H-%M-%S"`
archive_path="/maven/data/sdc/event-archive"
# archive_path="/tmp"

#Uncomment to remove archives older than 30 days
find $archive_path -mtime +30 -delete

archive_file="$archive_path/event_all_$current_time.csv"
/usr/bin/curl -s -X GET "http://sdc-web-maven.pdmz.lasp.colorado.edu/maven/sdc/service/events/api/v1/events/selected.csv" -o $archive_file
chmod 644 $archive_file
