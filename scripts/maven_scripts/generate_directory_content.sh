#! /usr/bin/env bash
#
# A bash script to record the directory contents for a given root
#
# Bryan Staley 2015-10-01

current_time=`date +"%Y-%m-%dT%H-%M-%S"`
archive_path="/maven/mavenpro/public-archive"
search_path="/maven/public"

#Uncomment to remove archives older than 30 days
find $archive_path -mtime +30 -delete

archive_file="${archive_path}/maven_public_structure-${current_time}.txt"

find ${search_path} > ${archive_file}

find_exit_code=$?

if [ $? -ne 0 ];then
  echo "Failed to generate ${archive_file} due to find returning ${find_exit_code}"
fi

exit ${find_exit_code}
