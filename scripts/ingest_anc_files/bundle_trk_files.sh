#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

python3 "$SCRIPT_DIR/bundle_trk_files.py" "-t" "full" "-o" "/maven/data/sdc/trk/"
