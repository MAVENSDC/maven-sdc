#! /usr/bin/env python
#
# A script generate TRK bundles
#
# Bryan Staley 2015-09-24

from ingest_anc_files import build_trk_bundle_main
from maven_status import job, MAVEN_SDC_COMPONENT

if __name__ == '__main__':
    with job.StatusCronJob(MAVEN_SDC_COMPONENT.ANC_INGESTER) as job:
        job.run(proc=build_trk_bundle_main.main,
                proc_args={})
