import re
from maven_utilities import constants
import os

ppi_search_url = "https://pds-ppi.igpp.ucla.edu/metadex/product/select?q=collection_id:%22urn:nasa:pds:"
ppi_suffix = "&rows=10000&indent=on&wt=json" # each query should end with this
atmos_search_url = "http://atmos.nmsu.edu/PDS/data/PDS4/MAVEN/"

env = os.environ.get(constants.python_env, 'production_readonly')

if env == 'testing':
    ppi_data_urls = []
    atmos_data_urls = []
else:
    ppi_data_urls = ["maven.euv.calibrated:data.bands%22",
                     "maven.euv.modelled:data.daily.spectra%22",
                     "maven.euv.modelled:data.minute.spectra%22",
		     "maven.euv.derived:occultations%22",
                     "maven.lpw.calibrated:data.lp.iv%22",
                     "maven.lpw.calibrated:data.mrg.scpot%22",
                     "maven.lpw.calibrated:data.w.e12%22",
                     "maven.lpw.calibrated:data.w.e12bursthf%22",
                     "maven.lpw.calibrated:data.w.e12burstlf%22",
                     "maven.lpw.calibrated:data.w.e12burstmf%22",
                     "maven.lpw.calibrated:data.w.specact%22",
                     "maven.lpw.calibrated:data.w.specpas%22",
                     "maven.lpw.derived:data.lp.nt%22",
                     "maven.lpw.derived:data.w.n%22",
                     "maven.lpw.raw:data.act%22",
                     "maven.lpw.raw:data.adr%22",
                     "maven.lpw.raw:data.atr%22",
                     "maven.lpw.raw:data.euv%22",
                     "maven.lpw.raw:data.hsbmhf%22",
                     "maven.lpw.raw:data.hsbmlf%22",
                     "maven.lpw.raw:data.hsbmmf%22",
                     "maven.lpw.raw:data.hsk%22",
                     "maven.lpw.raw:data.pas%22",
                     "maven.lpw.raw:data.spechfact%22",
                     "maven.lpw.raw:data.spechfpas%22",
                     "maven.lpw.raw:data.speclfact%22",
                     "maven.lpw.raw:data.speclfpas%22",
                     "maven.lpw.raw:data.specmfact%22",
                     "maven.lpw.raw:data.specmfpas%22",
                     "maven.lpw.raw:data.swp1%22",
                     "maven.lpw.raw:data.swp2%22",
                     "maven.mag.calibrated:data.pc%22",
                     "maven.mag.calibrated:data.pl%22",
                     "maven.mag.calibrated:data.ss%22",
                     "maven.insitu.calibrated:data.kp%22",
                     "maven.sep.calibrated:data.anc%22",
                     "maven.sep.calibrated:data.counts%22",
                     "maven.sep.calibrated:data.spec%22",
                     "maven.swea.calibrated:data.arc_3d%22",
                     "maven.swea.calibrated:data.arc_pad%22",
                     "maven.swea.calibrated:data.svy_3d%22",
                     "maven.swea.calibrated:data.svy_pad%22",
                     "maven.swea.calibrated:data.svy_spec%22",
                     "maven.swia.calibrated:data.coarse_arc_3d%22",
                     "maven.swia.calibrated:data.coarse_svy_3d%22",
                     "maven.swia.calibrated:data.fine_arc_3d%22",
                     "maven.swia.calibrated:data.fine_svy_3d%22",
                     "maven.swia.calibrated:data.onboard_svy_mom%22",
                     "maven.swia.calibrated:data.onboard_svy_spec%22",
                     "maven.static.c:data.2a_hkp%22",
                     "maven.static.c:data.c0_64e2m%22",
                     "maven.static.c:data.c2_32e32m%22",
                     "maven.static.c:data.c4_4e64m%22",
                     "maven.static.c:data.c6_32e64m%22",
                     "maven.static.c:data.c8_32e16d%22",
                     "maven.static.c:data.ca_16e4d16a%22",
                     "maven.static.c:data.cc_32e8d32m%22",
                     "maven.static.c:data.cd_32e8d32m%22",
                     "maven.static.c:data.ce_16e4d16a16m%22",
                     "maven.static.c:data.cf_16e4d16a16m%22",
                     "maven.static.c:data.d0_32e4d16a8m%22",
                     "maven.static.c:data.d1_32e4d16a8m%22",
                     "maven.static.c:data.d4_4d16a2m%22",
                     "maven.static.c:data.d6_events%22",
                     "maven.static.c:data.d7_fsthkp%22",
                     "maven.static.c:data.d8_12r1e%22",
                     "maven.static.c:data.d9_12r64e%22",
                     "maven.static.c:data.da_1r%22",
                     "maven.static.c:data.db_1024tof%22",
                     "maven.rose.calibrated:calibration.fup%22",
                     "maven.rose.calibrated:data.sky%22",
                     "maven.rose.raw:data.tnf%22",
                     "maven.rose.raw:data.rsr%22",
                     "maven.rose.raw:calibration.wea%22",
                     "maven.rose.raw:calibration.tro%22",
                     "maven.rose.raw:calibration.ion%22",
                     "maven.rose.raw:calibration.dlf%22",
                     "maven.rose.raw:browse.bro%22",
                     "maven.rose.derived:data.edp%22"]

    atmos_data_urls = ["iuvs_raw_bundle/l1a/cruise",
                       "iuvs_raw_bundle/calibration/centroid",
                       "iuvs_raw_bundle/l1a/corona",
                       "iuvs_raw_bundle/l1a/disk",
                       "iuvs_raw_bundle/l1a/echelle",
                       "iuvs_raw_bundle/l1a/limb",
                       "iuvs_raw_bundle/l1a/occultation",
                       "iuvs_raw_bundle/l1a/phobos",
                       "iuvs_raw_bundle/l1a/transition",
                       "iuvs_calibrated_bundle/l1b/cruise",
                       "iuvs_calibrated_bundle/calibration/centroid",
                       "iuvs_calibrated_bundle/l1b/corona",
                       "iuvs_calibrated_bundle/l1b/disk",
                       "iuvs_calibrated_bundle/l1b/echelle",
                       "iuvs_calibrated_bundle/l1b/limb",
                       "iuvs_calibrated_bundle/l1b/occultation",
                       "iuvs_calibrated_bundle/l1b/phobos",
                       "iuvs_calibrated_bundle/l1b/transition",
                       "iuvs_processed_bundle/l1c/corona",
                       "iuvs_processed_bundle/l1c/disk",
                       "iuvs_processed_bundle/l1c/echelle",
                       "iuvs_processed_bundle/l1c/limb",
                       "iuvs_processed_bundle/l1c/occultation",
                       "iuvs_derived_bundle/l2/corona",
                       "iuvs_derived_bundle/l2/disk",
                       "iuvs_derived_bundle/l2/limb",
                       "iuvs_derived_bundle/l2/occultation",
                       "iuvs-kp_bundle/kp/iuvs",
                       "ngims_bundle/l1a",
                       "ngims_bundle/l1b",
                       "ngims_bundle/l2",
                       "ngims_bundle/l3",
					   "acc_bundle/l3"]

general_basename_group = 'base'
general_version_group = 'version'
general_revision_group = 'revision'

pds_pattern = (r'mvn_(?:[a-zA-Z0-9]+)_'
                '(?:iuvs|insitu|ql|l[a-zA-Z0-9]+)'
                '(?:|_[a-zA-Z0-9\-]+)_'
                '(?:[0-9]{4})'
                '(?:[0-9]{2})'
                '(?:[0-9]{2})'
                '(?:|T[0-9]{6}|t[0-9]{6})_'
                'v(?:[0-9]+)_'
                'r(?:[0-9]+)')


pds_regex = re.compile(pds_pattern)

# General pattern to detect and process version/revision file names
base_verrev_pattern = (r'(?P<{0}>[a-zA-Z0-9_\-]+?)' 
                           '(?:_v(?P<{1}>[0-9]+))'
                           '(?:_r(?P<{2}>[0-9]+))').format(general_basename_group,
                                                            general_version_group,
                                                            general_revision_group)

base_verrev_regex = re.compile(base_verrev_pattern)
