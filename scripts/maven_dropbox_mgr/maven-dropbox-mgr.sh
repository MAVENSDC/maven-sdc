#! /bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Run the dropbox manager giving it a list of itf dropbox directories.
python3 "$SCRIPT_DIR/maven-dropbox-mgr.py" "/maven/itfhome/acc/dropbox"
python3 "$SCRIPT_DIR/maven-dropbox-mgr.py" "/maven/itfhome/iuvs/dropbox"
python3 "$SCRIPT_DIR/maven-dropbox-mgr.py" "/maven/itfhome/lpw/dropbox"
python3 "$SCRIPT_DIR/maven-dropbox-mgr.py" "/maven/itfhome/radio/dropbox"
python3 "$SCRIPT_DIR/maven-dropbox-mgr.py" "/maven/itfhome/euv/dropbox"
python3 "$SCRIPT_DIR/maven-dropbox-mgr.py" "/maven/itfhome/mag/dropbox" 
python3 "$SCRIPT_DIR/maven-dropbox-mgr.py" "/maven/itfhome/ngims/dropbox" 
python3 "$SCRIPT_DIR/maven-dropbox-mgr.py" "/maven/itfhome/pf/dropbox" 
python3 "$SCRIPT_DIR/maven-dropbox-mgr.py" "/maven/itfhome/mavenjpl/dropbox" 
python3 "$SCRIPT_DIR/maven-dropbox-mgr.py" "/maven/itfhome/sweal3/dropbox"
