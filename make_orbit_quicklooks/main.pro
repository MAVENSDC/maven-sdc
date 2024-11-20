;Grab the current directory
CD, CURRENT=c

;Tell IDL where to look for code
!PATH = !PATH + ':' +EXPAND_PATH('+'+c)
!PATH = !PATH + ':' +EXPAND_PATH('+/tools/spedas/')

;Register the SPICE toolkit
dlm_register, '/tools/icy/lib/icy.dlm' 

;Changes with new leap second
lsk_files = file_search("/maven/data/anc/spice/lsk/*")
sorted_lsk_files = lsk_files[sort(lsk_files)]
latest_lsk_file = sorted_lsk_files[-1]
cspice_furnsh, latest_lsk_file

;Should not change, these kernels describes Mars orbit.  Because they do not involve MAVEN, we don't store them on the SDC
cspice_furnsh, c+'/spice_kernels/mar097s.bsp'
cspice_furnsh, c+'/spice_kernels/de430s.bsp'

;This file describes the shape of Mars.  Probably will not change.  
cspice_furnsh, '/maven/data/anc/spice/pck/pck00009.tpc'

;This file describes the frames of MAVEN.  Probably will not change.  
cspice_furnsh, '/maven/data/anc/spice/fk/maven_v09.tf'

;This file converts spacecraft time to UTC.  This changes often.  
sclk_files = file_search("/maven/data/anc/spice/sclk/*")
sorted_sclk_files = sclk_files[sort(sclk_files)]
latest_sclk_file = sorted_sclk_files[-1]
cspice_furnsh, latest_sclk_file

;Contains the latest orbit information for MAVEN
cspice_furnsh, '/maven/data/anc/orb/maven_orb_rec.bsp'

idl_orbit_ql

exit