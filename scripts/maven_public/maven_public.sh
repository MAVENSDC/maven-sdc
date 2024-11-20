#! /bin/bash

#PUBLIC Release Window
export MAVEN_PUBLIC_QUERY_WINDOW_START=2014-09-22
export MAVEN_PUBLIC_QUERY_WINDOW_END=2016-05-15

# Run the maven data file indexer giving it the root directory of the data files.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Run the maven_public_main script to populate a MAVEN public site.
python3 "$SCRIPT_DIR/maven_public_main.py" "/maven/public/" "-s" "$@"
