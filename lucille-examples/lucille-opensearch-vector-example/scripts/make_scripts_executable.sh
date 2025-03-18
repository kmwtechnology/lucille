#!/bin/bash

# Directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Make all scripts in the script directory executable
echo "Making all scripts executable..."
find "${SCRIPT_DIR}" -name "*.sh" -exec chmod +x {} \;

echo "Done! All scripts are now executable."
