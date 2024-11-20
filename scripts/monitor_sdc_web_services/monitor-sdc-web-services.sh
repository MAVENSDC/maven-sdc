#! /bin/bash

# Run the maven data file indexer giving it the root directory of the data files.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Run the monitor sdc web services script to periodically check the health and status of the MAVEN web servers
python3 "$SCRIPT_DIR/monitor_sdc_web_services.py" $*
