import os

from collections import namedtuple
from maven_utilities import anc_config

invalid_dir = '/maven/data/sdc/misnamed_files/'

root_destination_directory = '/maven/data/anc/'
dns_tracking_dest_dir = 'trk'
dupe_dir_name = '/maven/data/sdc/duplicate_files/'

PatternDestinationRule = namedtuple('PatternDestinationRule', ['patterns', 'absolute_directory'])

file_rules = [
    PatternDestinationRule([anc_config.ancillary_regex,
                            anc_config.ancillary_eng_regex,
                            ],
                           lambda x, dest_dir:os.path.join(dest_dir, 'eng', x.group(anc_config.anc_product_group))),
    PatternDestinationRule([anc_config.anc_pred_sfp_regex,
                            anc_config.rec_app_regex,
                            ],
                           lambda x, dest_dir: os.path.join(dest_dir, 'eng', 'sff')),
    PatternDestinationRule([anc_config.anc_trk_regex],
                           lambda x, dest_dir: os.path.join(dest_dir, 'trk')),
    PatternDestinationRule([anc_config.optg_orb_regex,
                            anc_config.optg_orb_regex_2],
                           lambda x, dest_dir: os.path.join(dest_dir, 'optg')),


]

all_patterns = []

for _next in file_rules:
    all_patterns.extend(_next.patterns)

age_limit = 600.0  # ten minutes ago
