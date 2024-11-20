#!/bin/bash

# restore a set of files from AWS Glacier
# Author: Kim Kokkonen
# Created: 2014-09-30

if [ $# -lt 4 ]; then
  echo 'Illegal number of arguments!'
  echo 'Usage: aws_restore_files.sh vault_name restore_root_directory start_date end_date [skips]'
  echo 'Date format: yyyy-mm-dd limits archives downloaded based on CreationDate of the archive'
  echo 'If skips specified, that number of restore steps is skipped.'
  exit 255
fi

# debug -x, nodebug +x
set +x

source $HOME/mms_sdc/bin/activate
export PYTHON_ENV=production
# export PYTHONPATH=/code/maven_sdc/aws_archive_files
# export PYTHONPATH=/maven/mavenpro/software/python/lib/python2.7/site-packages

# set configuration variables from arguments
vault_name=$1
restore_root_directory=$2
start_date=$3
end_date=$4
if [ $# -eq 5 ]; then
  skips=$5
else
  skips=0
fi

# steps run so far
step_cnt=0

# submit inventory request to glacier
if [ $step_cnt -ge $skips ]; then
	python aws_submit_inventory_request.py $vault_name $start_date $end_date >job_id.txt
	status=$?
	if [ $status -ne 0 ]; then
	  exit $status
	fi
fi
step_cnt=$((step_cnt+1))
inv_job_id=`cat job_id.txt`

# wait for inventory request to be completed and download file when ready
if [ $step_cnt -ge $skips ]; then
	wait_flag=1
	while [ $wait_flag -gt 0 ]; do
	  python aws_download_inventory.py $vault_name $restore_root_directory $inv_job_id >inv_fn.txt
	  status=$?
	  if [ $status -eq 255 ]; then
	    exit $status
	  fi
	  if [ $status -eq 0 ]; then
	    wait_flag=0
	  else 
	    echo `date --rfc-3339=seconds` `cat inv_fn.txt`
	    sleep 60
	  fi
	done
fi
step_cnt=$((step_cnt+1))
inv_fn=`cat inv_fn.txt`

# submit download-archive requests
if [ $step_cnt -ge $skips ]; then
	python aws_submit_download_request.py $vault_name $restore_root_directory $inv_fn
	status=$?
	if [ $status -ne 0 ]; then
	  exit $status
	fi
fi
step_cnt=$((step_cnt+1))

# wait for archive downloads to be completed and stop when all are done
if [ $step_cnt -ge $skips ]; then
	wait_flag=1
	while [ $wait_flag -gt 0 ]; do
	  python aws_download_files.py $vault_name $restore_root_directory
	  status=$?
	  if [ $status -eq 255 ]; then
	    exit $status
	  fi
	  if [ $status -eq 254 ]; then
	    wait_flag=0
	  else
	    echo `date --rfc-3339=seconds` jobs in progress $status
	    sleep 60
	  fi
	done
fi
step_cnt=$((step_cnt+1))

# automate? untar the archives in time order, overwriting as we go
