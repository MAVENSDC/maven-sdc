#! /bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Run the dropbox manager giving it a list of itf dropbox directories.
python3 "$SCRIPT_DIR/maven-dropbox-mgr.py" "/maven/itfhome/acc/dropbox" \
                     "/maven/itfhome/iuvs/dropbox" \
                     "/maven/itfhome/lpw/dropbox" \
                     "/maven/itfhome/radio/dropbox" \
                     "/maven/itfhome/euv/dropbox" \
                     "/maven/itfhome/mag/dropbox" \
                     "/maven/itfhome/ngims/dropbox" \
                     "/maven/itfhome/pf/dropbox" \
                     "/maven/itfhome/mavenjpl/dropbox" \
                     "/maven/itfhome/sweal3/dropbox"
