
import argparse
import pytz
from ingest_anc_files import build_trk_bundle
from datetime import datetime
from dateutil.parser import parse
from maven_utilities import time_utilities


def main():
    parser = argparse.ArgumentParser(description='''Tool used to generate a tar bundle from received trk files''')
    parser.add_argument('-s', '--start-date')
    parser.add_argument('-e', '--end-date')
    parser.add_argument('-t', '--type-bundle',
                        choices=['full', 'latest', 'incremental', 'complete'],
                        default='complete')
    parser.add_argument('-i', '--increment-bundle',
                        type=float,
                        default=14.0)
    parser.add_argument('-p', '--print-manifest',
                        action='store_true',
                        help='Print the manifest')
    parser.add_argument('-m', '--manifest-file',
                        default=None,
                        help='Use this manifest')
    parser.add_argument('-o', '--output-dir',
                        help='The output directory for the TRK bundle file',
                        default='/maven/data/sdc/trk')

    args = parser.parse_args()

    if args.print_manifest:
        build_trk_bundle.dump_manifest(build_trk_bundle.get_manifest(manifest_file=args.manifest_file))
        return
    if args.type_bundle == 'latest':
        build_trk_bundle.build_bundle_latest(time_utilities.utc_now(), args.output_dir, args.increment_bundle, args.manifest_file)
    elif args.type_bundle == 'full':
        build_trk_bundle.build_bundle_full(time_utilities.utc_now(), args.output_dir, args.increment_bundle, args.manifest_file)
    elif args.type_bundle == 'incremental':
        start = parse(args.start_date).replace(tzinfo=pytz.UTC) if args.start_date is not None else datetime.min.replace(tzinfo=pytz.UTC)
        end = parse(args.end_date).replace(tzinfo=pytz.UTC) if args.end_date is not None else datetime.min.replace(tzinfo=pytz.UTC)
        build_trk_bundle.build_bundles(start, end, args.output_dir, args.increment_bundle, args.manifest_file)
    else:  # use provided start-date and end-date
        start = parse(args.start_date).replace(tzinfo=pytz.UTC) if args.start_date is not None else datetime.min.replace(tzinfo=pytz.UTC)
        end = parse(args.end_date).replace(tzinfo=pytz.UTC) if args.end_date is not None else datetime.min.replace(tzinfo=pytz.UTC)
        build_trk_bundle.build_bundle(start, end, args.output_dir, args.manifest_file)
