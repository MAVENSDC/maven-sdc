#! /usr/bin/env python
#
# A script to move the MAVEN ancillary data files from the directory where
# they arrive to their proper ancillary directories.
#
# Mike Dorey  2012-11-26
# Alexandria DeWolfe 2012-11-28
import sys
from ingest_anc_files.ingest_anc_files_main import main
from maven_status import job, MAVEN_SDC_COMPONENT

if __name__ == "__main__":
    with job.StatusCronJob(MAVEN_SDC_COMPONENT.ANC_INGESTER) as job:
        job.run(proc=main,
                proc_args={"arguments": sys.argv})
