## Ingest spice kernels module

This module provides the implementation for the Spice kernel dropbox.  
Spice kernels will be stored by file type:

lsk
sclk
pck
spk
ck
ik
fk

A script moves the MAVEN SPICE kernel files from the directory where the 
POC drops them to the destination SDC location: /maven/data/anc/spice/<file type>