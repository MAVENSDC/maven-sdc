#! /bin/bash

# Run the maven data file indexer giving it the root directory of the data files.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python3 "$SCRIPT_DIR/maven-data-file-indexer.py" "/maven/data/sci/ngi"
python3 "$SCRIPT_DIR/maven-data-file-indexer.py" "/maven/data/sci/acc"
python3 "$SCRIPT_DIR/maven-data-file-indexer.py" "/maven/data/sci/euv"
python3 "$SCRIPT_DIR/maven-data-file-indexer.py" "/maven/data/sci/iuv"
python3 "$SCRIPT_DIR/maven-data-file-indexer.py" "/maven/data/sci/kp"
python3 "$SCRIPT_DIR/maven-data-file-indexer.py" "/maven/data/sci/lpw"
python3 "$SCRIPT_DIR/maven-data-file-indexer.py" "/maven/data/sci/mag"
python3 "$SCRIPT_DIR/maven-data-file-indexer.py" "/maven/data/sci/pfp"
python3 "$SCRIPT_DIR/maven-data-file-indexer.py" "/maven/data/sci/radio"
python3 "$SCRIPT_DIR/maven-data-file-indexer.py" "/maven/data/sci/rse"
python3 "$SCRIPT_DIR/maven-data-file-indexer.py" "/maven/data/sci/sep"
python3 "$SCRIPT_DIR/maven-data-file-indexer.py" "/maven/data/sci/sta"
python3 "$SCRIPT_DIR/maven-data-file-indexer.py" "/maven/data/sci/swe"
python3 "$SCRIPT_DIR/maven-data-file-indexer.py" "/maven/data/sci/swi"
python3 "$SCRIPT_DIR/maven-data-file-indexer.py" "/maven/data/anc" 

