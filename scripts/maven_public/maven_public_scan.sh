#! /bin/bash

# Run the maven_public_scan_main script to clean the MAVEN public site.
maven_public_scan_main.py /maven/public/sci/ -r
maven_public_scan_main.py /maven/public/anc/ -r
