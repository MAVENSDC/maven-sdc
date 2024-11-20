import re

age_limit = 300.0  # five minutes ago

filename_pattern = re.compile(r'^mvn_([a-zA-Z]*)_ql_([0-9]{8})\.png')

source_root = '/maven/data/sci/'
public_source_root = '/maven/public/sci/'
pf_source = 'pfp,sep,sta,swe,swi,mag,lpw'
iuv_source = 'iuv'

destination_dir = '/maven/data/sdc/quicklook_files/'
public_destination_dir = '/maven/public/sdc/quicklook_files/'
