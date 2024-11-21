#! /usr/bin/env python
#
# A script to move the MAVEN SPICE kernels from the directory where
# they arrive to their proper ancillary directories.
#
# Mike Dorey  2012-11-26
from ingest_spice_kernels import utilities
from maven_status import job, MAVEN_SDC_COMPONENT


def main():
    utilities.ingest_spice_files()

if __name__ == "__main__":
    with job.StatusCronJob(MAVEN_SDC_COMPONENT.SPICE_INGESTER) as job:
        job.run(proc=main,
                proc_args={})
