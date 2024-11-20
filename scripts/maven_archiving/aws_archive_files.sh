#! /bin/bash

# Run the maven data file indexer giving it the root directory of the data files.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

python3 "$SCRIPT_DIR/aws_archive_files.py" "$@"

