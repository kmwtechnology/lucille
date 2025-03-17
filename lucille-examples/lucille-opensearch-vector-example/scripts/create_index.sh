#!/bin/bash

# Exit on error
set -e

# Source the environment setup
source "$(dirname "$0")/setup_environment.sh"

echo "Creating OpenSearch index: $OPENSEARCH_INDEX"

# Get the mapping file content
MAPPING_FILE="$(dirname "$0")/../mapping/opensearch_vector_mappings.json"
if [ ! -f "$MAPPING_FILE" ]; then
  echo "ERROR: Mapping file not found at $MAPPING_FILE"
  exit 1
fi

# Create the index with the mapping
curl -XPUT "$OPENSEARCH_URL$OPENSEARCH_INDEX" \
  -H 'Content-Type: application/json' \
  -d @"$MAPPING_FILE"

echo ""
echo "Index $OPENSEARCH_INDEX created successfully."
echo "You can now run the indexing process with: ./scripts/run_ingest.sh"
