#! /usr/bin/env python
'''
Functions used to provide the argument parsing for the make_pds_bundles scripts
Created on Mar 3, 2015

@author: bstaley
'''
import sys
import argparse

from make_pds_bundles import config, make_pds_bundles_main
from maven_status import job, MAVEN_SDC_COMPONENT
    
if __name__ == '__main__':
    args = make_pds_bundles_main.parse_arguments(sys.argv[1:])
    with job.StatusJob(MAVEN_SDC_COMPONENT.PDS_ARCHIVER, singleton=False) as job:
        job.run(proc=make_pds_bundles_main.main,
                proc_args={'date_range': args.date_range,
                           'root_dir': args.root_dir,
                           'instruments': args.instruments,
                           'dry_run':args.dry_run,
                           'print_instrument_config': args.print_instrument_config,
                           'print_instrument_lid': args.print_instrument_lid,
                           'report': args.report,
                           'notes':args.notes,
                           'override': args.override,
                           'skip_missing_labels':args.skip_missing_labels}
                           )
