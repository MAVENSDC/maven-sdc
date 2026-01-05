#!/usr/bin/env bash

# A script to update the SPEDAS library. 
# It is necessary to update the SPEDAS library regularly because SPEDAS is set to 
# deactivate itself after a certain period of time. 
#
# This script will:
# 1. Delete the existing SPEDAS directory.
# 2. Download the latest SPEDAS software zip file.
# 3. Unzip the software into the target directory.

# --- Configuration ---
# Use variables for paths to make the script easier to maintain.
SPEDAS_DIR="/maven/mavenpro/spedas"
DOWNLOAD_URL="https://spedas.org/downloads/spdsw_latest.zip"
TEMP_ZIP_FILE=$(mktemp --suffix=.zip)

# --- Script Execution ---

# Exit immediately if a command exits with a non-zero status, an unset variable is used, or a pipeline fails.
set -euo pipefail

# Function to clean up the temporary zip file on exit
cleanup() {
  echo "---"
  echo "Cleaning up temporary files..."
  rm -f "$TEMP_ZIP_FILE"
  echo "Cleanup complete."
  }

# Register the cleanup function to be called on script exit
trap cleanup EXIT

echo "Starting SPEDAS update and quicklook generation process..."
echo "---"

# Check for required commands
if ! command -v wget &> /dev/null; then
    echo "Error: 'wget' is required but not found in PATH."
    exit 1
fi

if ! command -v unzip &> /dev/null; then
    echo "Error: 'unzip' is required but not found in PATH."
    exit 1
fi
### Step 1
### Delete the old SPEDAS directory
echo "Step 1: Deleting old directory: $SPEDAS_DIR"

# Validate the SPEDAS_DIR variable to prevent accidental data loss
if [ -z "$SPEDAS_DIR" ] || [ "$SPEDAS_DIR" = "/" ] || [ "$SPEDAS_DIR" = "/home" ]; then
    echo "Error: SPEDAS_DIR is not set correctly or points to a critical directory."
    exit 1
fi
# The 'if' statement prevents an error if the directory doesn't exist
rm -rf "$SPEDAS_DIR"

### Step 2
### Download the latest code
echo "Step 2: Downloading latest SPEDAS from $DOWNLOAD_URL"
wget --quiet --tries=3 --timeout=30 -O "$TEMP_ZIP_FILE" "$DOWNLOAD_URL"
echo "Download complete."
echo "---"

### Step 3 
### Unzip the file into the target directory
echo "Step 3: Unzipping $TEMP_ZIP_FILE to $SPEDAS_DIR"
unzip -q "$TEMP_ZIP_FILE" -d "$SPEDAS_DIR"
echo "Unzip complete."
echo "---"

# The 'trap' will handle cleanup automatically now.
exit 0