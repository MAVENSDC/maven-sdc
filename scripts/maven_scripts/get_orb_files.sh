#!/bin/bash

# Download all the MAVEN orbit files we care about from JPL
# Kim Kokkonen, 2015-06-12
# Heather Cronk, 2023-08-09: Update for https instead of ftp

# local_basedir is assumed to exist
local_basedir=/maven/data/anc/orb
https_listdir=$(curl -s https://naif.jpl.nasa.gov/pub/naif/MAVEN/kernels/spk/ | grep -o 'href=".*">' | sed -e "s/href=\"//g" | sed -e "s/\">//g")

for f in $https_listdir; do
  if [[ $f == maven_orb*.orb || $f == maven_orb*.bsp ]] ; then
    curl -s -o $local_basedir/$f -z $local_basedir/$f https://naif.jpl.nasa.gov/pub/naif/MAVEN/kernels/spk/$f
    chmod 644 $local_basedir/$f
  fi
done
