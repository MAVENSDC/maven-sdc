#!/bin/bash
#set -e causes the script to immediately exit if the return code of any command is not 0. Note: If you're using this elsewhere there are caveats.
set -e

# Run the maven data file indexer giving it the root directory of the data files.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

python3 "$SCRIPT_DIR/ingest-l0-files.py" "/maven/poc/" "/maven/data/sci/"
python3 "$SCRIPT_DIR/ingest-spice-kernels.py"
python3 "$SCRIPT_DIR/ingest-anc-files.py" "/maven/poc/anc/"
