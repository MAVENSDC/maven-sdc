#! /bin/bash
#
# This is a script that runs the maven-in-situ-kp-file-ingester.py script.
# It is meant to be run in cron.
#
# Kim Kokkonen 2014-09-02

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

python3 "$SCRIPT_DIR/maven-in-situ-kp-file-ingester.py" "/maven/data/sci/kp/insitu"
