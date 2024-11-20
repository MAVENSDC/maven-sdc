#!/bin/bash
# monitor.sh
# Scans the MAVEN SDC data storage to determine if new files have been uploaded. An HTML output is provided
# to report processed files.
#
# Python scripts:
#
# Arguments:
# 1 - Base directory to scan
# 2 - HTML output file
# Example: new-files.py [Base dir] [ Output HTML file ]
#
# new-files.py - A script used to determine if any new files were provided
# Arguments:
# 1 - Base directory to scan
# 2 - True/False True indicates that a sub-directory scan should happen.  False indicates that only the specified directory will be searched
# 3 - HTML output file
# 4 - maximum number of entries to display, <=0 means all
# Example: new-files.py [Base dir] [ Output HTML file ]
#
# quicklook.py - A script used to determine if any new quicklook files for a particular instrument were provided and generate HTML output 
# listing the quicklook files found for the instrument.
# Arguments:
# 1 - Instrument to process
# Example: quicklook.py [Instrument]

#set -e causes the script to immediately exit if the return code of any command is not 0. Note: If you're using this elsewhere there are caveats.  
set -e


new-files.py '/maven/data/sci/iuv/l0' True /maven/data/sdc/new_sci_files/iuvsl0.html 30
new-files.py '/maven/data/sci/iuv/l1a' True /maven/data/sdc/new_sci_files/iuvsl1a.html 30
new-files.py '/maven/data/sci/iuv/l1b' True /maven/data/sdc/new_sci_files/iuvsl1b.html 30
new-files.py '/maven/data/sci/iuv/l1c' True /maven/data/sdc/new_sci_files/iuvsl1c.html 30
new-files.py '/maven/data/sci/iuv/l2' True /maven/data/sdc/new_sci_files/iuvsl2.html 30
new-files.py '/maven/data/sci/iuv/l3' True /maven/data/sdc/new_sci_files/iuvsl3.html 30

new-files.py '/maven/data/sci/pfp/ql' False /maven/data/sdc/new_sci_files/pfql.html 30

new-files.py '/maven/data/sci/pfp/l0' True /maven/data/sdc/new_sci_files/pfpl0.html 30
new-files.py '/maven/data/sci/mag/l0' True /maven/data/sdc/new_sci_files/magl0.html 30

new-files.py '/maven/data/sci/ngi/l0' True /maven/data/sdc/new_sci_files/ngimsl0.html 30

new-files.py '/maven/data/sci/acc/l2' True /maven/data/sdc/new_sci_files/accl2.html 30
new-files.py '/maven/data/sci/acc/l3' True /maven/data/sdc/new_sci_files/accl3.html 30
new-files.py '/maven/data/sci/acc/aag' True /maven/data/sdc/new_sci_files/aag.html 30

new-files.py '/maven/data/sci/lpw/l2' True /maven/data/sdc/new_sci_files/lpwl2.html 30
new-files.py '/maven/data/sci/mag/l2' True /maven/data/sdc/new_sci_files/magl2.html 30
new-files.py '/maven/data/sci/sep/l2' True /maven/data/sdc/new_sci_files/sepl2.html 30
new-files.py '/maven/data/sci/sta/l2' True /maven/data/sdc/new_sci_files/stal2.html 30
new-files.py '/maven/data/sci/swe/l2' True /maven/data/sdc/new_sci_files/swel2.html 30
new-files.py '/maven/data/sci/swi/l2' True /maven/data/sdc/new_sci_files/swil2.html 30
new-files.py '/maven/data/sci/euv/l2' True /maven/data/sdc/new_sci_files/euvl2.html 30

new-files.py '/maven/data/sci/euv/l3' True /maven/data/sdc/new_sci_files/pfpl3.html 30
new-files.py '/maven/data/sci/ngi/l2' True /maven/data/sdc/new_sci_files/ngimsl2.html 30
new-files.py '/maven/data/sci/ngi/l3' True /maven/data/sdc/new_sci_files/ngimsl3.html 30

new-files.py '/maven/data/anc/spice/ck' False /maven/data/sdc/new_anc_files/ck.html 30
new-files.py '/maven/data/anc/spice/fk' False /maven/data/sdc/new_anc_files/fk.html 30
new-files.py '/maven/data/anc/spice/ik' False /maven/data/sdc/new_anc_files/ik.html 30
new-files.py '/maven/data/anc/spice/lsk' False /maven/data/sdc/new_anc_files/lsk.html 30
new-files.py '/maven/data/anc/spice/pck' False /maven/data/sdc/new_anc_files/pck.html 30
new-files.py '/maven/data/anc/spice/sclk' False /maven/data/sdc/new_anc_files/sclk.html 30
new-files.py '/maven/data/anc/spice/spk' False /maven/data/sdc/new_anc_files/spk.html 30
new-files.py '/maven/data/anc/optg' False /maven/data/sdc/new_anc_files/optg.html 30
new-files.py '/maven/data/anc/eng/sff' False /maven/data/sdc/new_anc_files/sff.html 30
new-files.py '/maven/data/anc/eng/imu' False /maven/data/sdc/new_anc_files/imu.html 30
new-files.py '/maven/data/anc/eng/eps' False /maven/data/sdc/new_anc_files/eps.html 30
new-files.py '/maven/data/anc/eng/gnc' False /maven/data/sdc/new_anc_files/gnc.html 30
new-files.py '/maven/data/anc/eng/pte' False /maven/data/sdc/new_anc_files/pte.html 30
new-files.py '/maven/data/anc/eng/ngms' False /maven/data/sdc/new_anc_files/ngms.html 30
new-files.py '/maven/data/anc/eng/pf' False /maven/data/sdc/new_anc_files/pf.html 30
new-files.py '/maven/data/anc/eng/rs' False /maven/data/sdc/new_anc_files/rs.html 30

new-files.py '/maven/data/anc/eng/sasm1' False /maven/data/sdc/new_anc_files/sasm1.html 30
new-files.py '/maven/data/anc/eng/sasm2' False /maven/data/sdc/new_anc_files/sasm2.html 30
new-files.py '/maven/data/anc/eng/sasm3' False /maven/data/sdc/new_anc_files/sasm3.html 30

new-files.py '/maven/data/anc/eng/usm1' False /maven/data/sdc/new_anc_files/usm1.html 30
new-files.py '/maven/data/anc/eng/usm2' False /maven/data/sdc/new_anc_files/usm2.html 30
new-files.py '/maven/data/anc/eng/usm3' False /maven/data/sdc/new_anc_files/usm3.html 30
new-files.py '/maven/data/anc/eng/usm4' False /maven/data/sdc/new_anc_files/usm4.html 30
new-files.py '/maven/data/anc/eng/usm5' False /maven/data/sdc/new_anc_files/usm5.html 30
new-files.py '/maven/data/anc/eng/usm6' False /maven/data/sdc/new_anc_files/usm6.html 30

new-files.py '/maven/data/anc/trk' False /maven/data/sdc/new_anc_files/trk.html 30

new-files.py '/maven/data/sci/kp/iuvs' False /maven/data/sdc/new_sci_files/iuvskp.html 30
new-files.py '/maven/data/sci/kp/insitu' False /maven/data/sdc/new_sci_files/iskp.html 30

quicklook-files.py 'pfp'
quicklook-files.py 'iuv'

