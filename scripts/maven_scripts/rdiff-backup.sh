#! /usr/bin/env bash
#
# A script to run rdiff-backup of /maven/itfhome.
#
# Mike Dorey   2013-06-14
# Kim Kokkonen 2014-09-09 exclude all hidden files
# Bryan Staley 2021-05-20 removing dropbox from diff list

rdiff-backup --exclude '**/.*' \
             --exclude '**/emtool' \
             --exclude '**/dropbox' \
             --exclude '**/spice_dropbox' \
             --exclude '**/misnamed_files' \
             --exclude '**/duplicate_files' \
             /maven/itfhome /maven/mavenpro/rdiff-backups/itfhome
#rdiff-backup --exclude /maven/mavenpro/rdiff-backups --exclude "**/.*" /maven /maven/mavenpro/rdiff-backups/maven

